package warrenfalk.fuselaj.example;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;

import warrenfalk.fuselaj.DirBuffer;
import warrenfalk.fuselaj.FileInfo;
import warrenfalk.fuselaj.Filesystem;
import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.fuselaj.Mode;
import warrenfalk.fuselaj.Stat;
import warrenfalk.fuselaj.Errno;

public class ExampleFs extends Filesystem {
	DirEntry root;
	Hashtable<Long, Inode> inodes = new Hashtable<Long, Inode>();
	long nextInode = 1;
	
	public ExampleFs() {
		Directory rootdir = new Directory();
		Inode rootinode = new Inode();
		rootinode.data = rootdir;
		root = new DirEntry("/", rootinode, 0755);
		
		Inode hw = new Inode();
		hw.data = "Hello World!";
		rootdir.entries.add(new DirEntry("hello", hw, 0444));
	}
	
	class Inode {
		long inode;
		int links;
		long ctime;
		long mtime;
		long atime;
		Object data;
		
		public Inode() {
			synchronized (ExampleFs.this) {
				inode = nextInode++;
				inodes.put(inode, this);
			}
		}
		
		long size() {
			if (data instanceof Directory)
				return ((Directory)data).entries.size();
			if (data instanceof String)
				return ((String)data).length();
			else
				return 0;
		}
	}
	
	static class DirEntry {
		String name;
		long inode;
		int mode;
		
		public DirEntry(String name, Inode inode, int mode) {
			this.name = name;
			inode.links++;
			this.inode = inode.inode;
			if (inode.data instanceof Directory)
				mode |= Mode.IFDIR;
			else
				mode |= Mode.IFREG;
			this.mode = mode;
		}
	}
	
	static class Directory {
		ArrayList<DirEntry> entries;
		int mode;
		
		public Directory() {
			entries = new ArrayList<DirEntry>();
			mode = 0755;
		}
		
		DirEntry find(String name) {
			for (DirEntry entry : entries)
				if (entry.name.equals(name))
					return entry;
			return null;
		}

		public DirEntry remove(String name) throws FilesystemException {
			for (int i = 0; i < entries.size(); i++)
				if (entries.get(i).name.equals(name))
					return entries.remove(i);
			throw new FilesystemException(Errno.NoSuchFileOrDirectory);
		}
	}
	
	class FileData {
		public FileData() {
		}
	}
	
	public static void main(String[] args) {
		ExampleFs fs = new ExampleFs();
		int exitCode = fs.run(args);
		System.exit(exitCode);
	}
	
	DirEntry getDirEntry(String path) throws FilesystemException {
		DirEntry entry = root;
		int slash = 0;
		while ((slash + 1) < path.length()) {
			path = path.substring(slash + 1);
			Inode inode = inodes.get(entry.inode);
			if (!(inode.data instanceof Directory))
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
			Directory dir = (Directory)inode.data;
			slash = path.indexOf('/');
			if (slash == -1)
				slash = path.length();
			String name = path.substring(0, slash);
			entry = dir.find(name);
			if (entry == null)
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
		}
		return entry;
	}
	
	@Override
	protected void getattr(String path, Stat stat) throws FilesystemException {
		// TODO: remove test code
		if (path.contains("/special")) {
			stat.putLinkCount(2);
			stat.putMode(Mode.IFDIR | 0755);
			stat.putInode(0);
			return;
		}
		DirEntry entry = getDirEntry(path);
		stat.putMode(entry.mode);
		stat.putInode(entry.inode);
		Inode inode = inodes.get(entry.inode);
		stat.putLinkCount(inode.links);
		stat.putSize(inode.size());
	}
	
	@Override
	protected void readdir(String path, DirBuffer dirBuffer, FileInfo fileInfo) throws FilesystemException {
		// TODO: remove test code
		if (path.endsWith("/special")) {
			long i = dirBuffer.getPosition() + 1;
			while (i < 500) {
				System.out.println(i);
				if (dirBuffer.putDir("dir" + i, i))
					return;
				i++;
			}
			return;
		}
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (!(inode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory dir = (Directory)inode.data;
		dirBuffer.putDir(".", entry.inode, entry.mode, 0);
		// TODO: get the parent directory
		dirBuffer.putDir("..", 0);
		for (DirEntry child : dir.entries)
			dirBuffer.putDir(child.name, child.inode, child.mode, 0);
	}
	
	@Override
	protected void open(String path, FileInfo fileInfo) throws FilesystemException {
		System.err.println("*** open(" + path + ", FileInfo {fh=" + fileInfo.getFileHandle() + ",lock=" + fileInfo.getLockOwner() + ",oflags=" + fileInfo.getOpenFlags() + ",writepage=" + fileInfo.getWritePage() + "})");
	}
	
	@Override
	protected void read(String path, FileInfo fileInfo, ByteBuffer buffer, long position) throws FilesystemException {
		int len = 0;
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (inode.data instanceof String) {
			byte[] bytes = ((String)inode.data).getBytes();
			len = bytes.length - (int)position;
			if (len > buffer.remaining())
				len = buffer.remaining();
			buffer.put(bytes, (int)position, len);
		}
	}
	
	@Override
	protected void mkdir(String path, int mode) throws FilesystemException {
		// first break path into parent and name
		int slash = path.lastIndexOf('/');
		String parent = path.substring(0, slash);
		if (parent.length() == 0)
			parent = "/";
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if new dir already exists
		String name = path.substring(slash + 1);
		DirEntry entry = parentDir.find(name);
		if (entry != null)
			throw new FilesystemException(Errno.FileExists);
		// create directory
		Directory dir = new Directory();
		Inode inode = new Inode();
		inode.data = dir;
		parentDir.entries.add(new DirEntry(name, inode, mode));
	}
	
	@Override
	protected void rmdir(String path) throws FilesystemException {
		// first break path into parent and name
		int slash = path.lastIndexOf('/');
		String parent = path.substring(0, slash);
		if (parent.length() == 0)
			parent = "/";
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if new dir exists
		String name = path.substring(slash + 1);
		DirEntry entry = parentDir.find(name);
		Inode inode = inodes.get(entry.inode);
		Directory dir = (Directory)inode.data;
		if (dir.entries.size() > 0)
			throw new FilesystemException(Errno.DirectoryNotEmpty);
		parentDir.remove(name);
	}
}
