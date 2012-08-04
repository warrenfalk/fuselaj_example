package warrenfalk.fuselaj.example;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicLong;

import warrenfalk.fuselaj.DirBuffer;
import warrenfalk.fuselaj.FileInfo;
import warrenfalk.fuselaj.FuselajFs;
import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.fuselaj.FuseContext;
import warrenfalk.fuselaj.Mode;
import warrenfalk.fuselaj.Stat;
import warrenfalk.fuselaj.Errno;
import warrenfalk.fuselaj.StatVfs;

public class ExampleFs extends FuselajFs {
	DirEntry root;
	Hashtable<Long, Inode> inodes = new Hashtable<Long, Inode>();
	long nextInode = 1;
	
	Path rootPath = nfs.getPath(".").normalize();
	
	public ExampleFs() {
		super(false);
		Directory rootdir = new Directory();
		Inode rootinode = new Inode(rootdir, 0755);
		root = new DirEntry(rootPath, rootinode);
		
		Inode hw = new Inode("Hello World!", 0444);
		rootdir.entries.add(new DirEntry(nfs.getPath("hello"), hw));
	}
	
	class Inode {
		long inode;
		int links;
		long ctime;
		long mtime;
		long atime;
		Object data;
		int mode;
		int uid;
		int gid;
		
		public Inode(Object data, int mode) {
			fileCount.incrementAndGet();
			mtime = ctime = atime = getUnixTime(Calendar.getInstance().getTimeInMillis());
			synchronized (ExampleFs.this) {
				inode = nextInode++;
				inodes.put(inode, this);
			}
			this.data = data;
			if (data instanceof Directory)
				mode |= Mode.IFDIR;
			else
				mode |= Mode.IFREG;
			this.mode = mode;
			FuseContext context = FuseContext.getCurrent();
			if (context != null) {
				this.uid = context.getUserId();
				this.gid = context.getGroupId();
			}
		}
		
		@Override
		protected void finalize() throws Throwable {
			fileCount.decrementAndGet();
			super.finalize();
		}
		
		long size() {
			if (data instanceof Directory)
				return ((Directory)data).entries.size(); // note: other filesystems give the sum of the sizes of the entries
			if (data instanceof String)
				return ((String)data).length();
			if (data instanceof FileData)
				return ((FileData)data).size();
			if (data instanceof SymLink)
				return ((SymLink)data).target.toString().length();
			return 0;
		}
	}
	
	static class SymLink {
		final public Path target;
		
		public SymLink(Path target) {
			this.target = target;
		}
	}

	static class DirEntry {
		Path name;
		long inode;
		
		public DirEntry(Path name, Inode inode) {
			this.name = name;
			inode.links++;
			this.inode = inode.inode;
		}
	}
	
	static class Directory {
		ArrayList<DirEntry> entries;
		int mode;
		
		public Directory() {
			entries = new ArrayList<DirEntry>();
			mode = 0755;
		}
		
		DirEntry find(Path name) {
			for (DirEntry entry : entries)
				if (entry.name.equals(name))
					return entry;
			return null;
		}

		public DirEntry remove(Path name) throws FilesystemException {
			for (int i = 0; i < entries.size(); i++)
				if (entries.get(i).name.equals(name))
					return entries.remove(i);
			throw new FilesystemException(Errno.NoSuchFileOrDirectory);
		}

		public void add(DirEntry entry) {
			entries.add(entry);
		}
	}
	
	static AtomicLong blockCount = new AtomicLong();
	static AtomicLong fileCount = new AtomicLong();
	
	class FileData {
		long size;
		HashMap<Long,byte[]> blocks;
		
		final static int BLOCKSIZE = 0x1000;
		
		public FileData() {
			blocks = new HashMap<Long,byte[]>();
			size = 0;
		}
		
		public long size() {
			return size;
		}
		
		public void write(ByteBuffer bb, long offset) {
			long newSize = bb.remaining() + offset;
			if (newSize > size)
				size = newSize;
			// which buffer do we start in?
			while (bb.remaining() > 0) {
				long blocknum = offset / BLOCKSIZE;
				int blockoffset = (int)(offset % BLOCKSIZE);
				byte[] block = blocks.get(blocknum);
				if (block == null) {
					blocks.put(blocknum, block = new byte[BLOCKSIZE]);
					blockCount.incrementAndGet();
				}
				int len = BLOCKSIZE - blockoffset;
				if (len > bb.remaining())
					len = bb.remaining();
				bb.get(block, blockoffset, len);
				offset += len;
			}
		}

		public void read(ByteBuffer bb, long offset) {
			long remain = size - offset;
			while (bb.remaining() > 0 && remain > 0) {
				long blocknum = offset / BLOCKSIZE;
				int blockoffset = (int)(offset % BLOCKSIZE);
				byte[] block = blocks.get(blocknum);
				if (block == null)
					return;
				int len = BLOCKSIZE - blockoffset;
				if (len > remain)
					len = (int)remain;
				if (len > bb.remaining())
					len = bb.remaining();
				bb.put(block, blockoffset, len);
				offset += len;
				remain -= len;
			}
		}

		public void truncate(long size) {
			this.size = size;
			synchronized (blocks) {
				Long[] keys = blocks.keySet().toArray(new Long[blocks.size()]);
				for (long key : keys) {
					long end = key + BLOCKSIZE;
					if (size < end)
						continue;
					blocks.remove(key);
					blockCount.decrementAndGet();
				}
			}
		}
	}
	
	static class FileHandle {
		final long number;
		final Inode inode;
		
		private static AtomicLong nextHandle = new AtomicLong();
		private static HashMap<Long,FileHandle> map = new HashMap<Long,FileHandle>();
		
		private FileHandle(Inode inode) {
			this.number = nextHandle.incrementAndGet();
			this.inode = inode;
		}
		
		public static FileHandle open(FileInfo fi, Inode inode) {
			FileHandle handle = new FileHandle(inode);
			fi.putFileHandle(handle.number);
			synchronized (map) {
				map.put(handle.number, handle);
			}
			return handle;
		}
		
		public static FileHandle release(FileInfo fi) {
			FileHandle handle;
			synchronized (map) {
				handle = map.remove(fi.getFileHandle());
			}
			return handle;
		}
		
		public static FileHandle get(long number) {
			synchronized (map) {
				return map.get(number);
			}
		}
	}
	
	public static void main(String[] args) {
		ExampleFs fs = new ExampleFs();
		int exitCode = fs.run(args);
		System.exit(exitCode);
	}
	
	DirEntry getDirEntry(Path path) throws FilesystemException {
		DirEntry entry = root;
		while (!rootPath.equals(path) && path != null) {
			Inode inode = inodes.get(entry.inode);
			if (!(inode.data instanceof Directory))
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
			Directory dir = (Directory)inode.data;
			Path name = path.getName(0);
			entry = dir.find(name);
			if (entry == null)
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
			if (path.getNameCount() == 1)
				break;
			path = path.subpath(1, path.getNameCount());
		}
		return entry;
	}
	
	@Override
	protected void getattr(Path path, Stat stat) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		stat.putInode(entry.inode);
		Inode inode = inodes.get(entry.inode);
		stat.putMode(inode.mode);
		stat.putLinkCount(inode.links);
		stat.putSize(inode.size());
		stat.putCTime(inode.ctime);
		stat.putModTime(inode.mtime);
		stat.putAccessTime(inode.atime);
		stat.putUserId(inode.uid);
		stat.putGroupId(inode.gid);
	}
	
	@Override
	protected void opendir(Path path, FileInfo fileInfo) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (!(inode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		FileHandle.open(fileInfo, inode);
	}
	
	@Override
	protected void readdir(Path path, DirBuffer dirBuffer, FileInfo fileInfo) throws FilesystemException {
		FileHandle fh = FileHandle.get(fileInfo.getFileHandle());
		Inode inode = fh.inode;
		Directory dir = (Directory)inode.data;
		dirBuffer.putDir(".", inode.inode, inode.mode, 0);
		// TODO: get the parent directory
		dirBuffer.putDir("..", 0);
		for (DirEntry child : dir.entries) {
			Inode childnode = inodes.get(child.inode);
			dirBuffer.putDir(child.name.toString(), child.inode, childnode.mode, 0);
		}
	}
	
	@Override
	protected void releasedir(Path path, FileInfo fi) throws FilesystemException {
		FileHandle.release(fi);
	}
	
	@Override
	protected void read(Path path, FileInfo fileInfo, ByteBuffer buffer, long position) throws FilesystemException {
		FileHandle fh = FileHandle.get(fileInfo.getFileHandle());
		Inode inode = fh.inode;
		if (inode.data instanceof FileData) {
			FileData data = (FileData)inode.data;
			synchronized (data) {
				data.read(buffer, position);
			}
		}
		else if (inode.data instanceof String) {
			byte[] bytes = ((String)inode.data).getBytes();
			int len = bytes.length - (int)position;
			if (len > buffer.remaining())
				len = buffer.remaining();
			buffer.put(bytes, (int)position, len);
		}
		else {
			throw new FilesystemException(Errno.PermissionDenied);
		}
	}
	
	@Override
	protected void mkdir(Path path, int mode) throws FilesystemException {
		// first break path into parent and name
		Path parent = parentOf(path);
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if new dir already exists
		Path name = path.getFileName();
		DirEntry entry = parentDir.find(name);
		if (entry != null)
			throw new FilesystemException(Errno.FileExists);
		// create directory
		Directory dir = new Directory();
		Inode inode = new Inode(dir, mode);
		parentDir.add(new DirEntry(name, inode));
	}
	
	@Override
	protected void rmdir(Path path) throws FilesystemException {
		// first break path into parent and name
		Path parent = parentOf(path);
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if new dir exists
		Path name = path.getFileName();
		DirEntry entry = parentDir.find(name);
		Inode inode = inodes.get(entry.inode);
		Directory dir = (Directory)inode.data;
		if (dir.entries.size() > 0)
			throw new FilesystemException(Errno.DirectoryNotEmpty);
		parentDir.remove(name);
	}
	
	@Override
	protected void access(Path path, int mask) throws FilesystemException {
		// for now we just give access to everything
		/* I think the way this should work is that we use this function to check access from all other functions.
		 * Doing it this way allows other applications to check access, then, using the access() system call and always
		 * get accurate results
		 */
	}
	
	@Override
	protected void chmod(Path path, int mode) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		inode.mode = mode;
	}
	
	@Override
	protected void chown(Path path, int uid, int gid) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (uid != -1)
			inode.uid = uid;
		if (gid != -1)
			inode.gid = gid;
	}
	
	@Override
	protected void fgetattr(Path path, Stat stat, FileInfo fi) throws FilesystemException {
		super.fgetattr(path, stat, fi);
	}
	
	@Override
	protected void open(Path path, FileInfo fileInfo) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		FileHandle.open(fileInfo, inode);
	}
	
	@Override
	protected void create(Path path, int mode, FileInfo fi) throws FilesystemException {
		Inode inode = makeNode(path, mode, 0);
		FileHandle.open(fi, inode);
	}
	
	@Override
	protected void release(Path path, FileInfo fi) throws FilesystemException {
		FileHandle.release(fi);
	}
	
	@Override
	protected void write(Path path, FileInfo fi, ByteBuffer bb, long offset) throws FilesystemException {
		FileHandle fh = FileHandle.get(fi.getFileHandle());
		Inode inode = fh.inode;
		if (inode.data instanceof FileData) {
			FileData data = (FileData)inode.data;
			synchronized (data) {
				data.write(bb, offset);
			}
		}
		else {
			throw new FilesystemException(Errno.PermissionDenied);
		}
	}
	
	@Override
	protected void utimens(Path path, long accessSeconds, long accessNanoseconds, long modSeconds, long modNanoseconds) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		inode.mtime = modSeconds;
		inode.atime = accessSeconds;
	}
	
	@Override
	protected void truncate(Path path, long size) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (inode.data instanceof FileData) {
			FileData data = (FileData)inode.data;
			synchronized (data) {
				data.truncate(size);
			}
		}
		else {
			throw new FilesystemException(Errno.PermissionDenied);
		}
	}
	
	@Override
	protected void ftruncate(Path path, long size, FileInfo fi) throws FilesystemException {
		FileHandle fh = FileHandle.get(fi.getFileHandle());
		Inode inode = fh.inode;
		if (inode.data instanceof FileData) {
			FileData data = (FileData)inode.data;
			synchronized (data) {
				data.truncate(size);
			}
		}
		else {
			throw new FilesystemException(Errno.PermissionDenied);
		}
	}
	
	@Override
	protected void link(Path from, Path to) throws FilesystemException {
		DirEntry fromEntry = getDirEntry(from);
		Inode fromInode = inodes.get(fromEntry.inode);
		// first break path into parent and name
		
		Path parent = parentOf(to);
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if file already exists
		Path name = to.getFileName();
		DirEntry entry = parentDir.find(name);
		if (entry != null)
			throw new FilesystemException(Errno.FileExists);
		// create file
		parentDir.entries.add(new DirEntry(name, fromInode));
	}
	
	@Override
	protected void symlink(Path targetOfLink, Path pathOfLink) throws FilesystemException {
		// first break path into parent and name
		Path parent = parentOf(pathOfLink);
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if file already exists
		Path name = pathOfLink.getFileName();
		DirEntry entry = parentDir.find(name);
		if (entry != null)
			throw new FilesystemException(Errno.FileExists);
		// create file
		SymLink target = new SymLink(targetOfLink);
		Inode inode = new Inode(target, Mode.IFLNK | 0777);
		parentDir.entries.add(new DirEntry(name, inode));
	}
	
	@Override
	protected Path readlink(Path path) throws FilesystemException {
		DirEntry entry = getDirEntry(path);
		Inode inode = inodes.get(entry.inode);
		if (!(inode.data instanceof SymLink))
			throw new FilesystemException(Errno.InvalidArgument);
		return ((SymLink)inode.data).target;
	}
	
	@Override
	protected void rename(Path from, Path to) throws FilesystemException {
		Path parent;
		DirEntry parentEntry;
		Inode parentInode;

		// get the from directory
		parent = parentOf(from);
		parentEntry = getDirEntry(parent);
		parentInode = inodes.get(parentEntry.inode);
		Directory fromParent = (Directory)parentInode.data;
		Path fromName = from.getFileName();
		
		// get the to directory
		parent = parentOf(to);
		parentEntry = getDirEntry(parent);
		parentInode = inodes.get(parentEntry.inode);
		Directory toParent = (Directory)parentInode.data;
		Path toName = to.getFileName();
		
		DirEntry entry = fromParent.find(fromName);
		entry.name = toName;
		// if they have different parents, move it to the new parent
		if (fromParent != toParent)
			toParent.add(fromParent.remove(fromName));
	}
	
	protected Inode makeNode(Path path, int mode, long rdev) throws FilesystemException {
		Path parent = parentOf(path);
		// get the parent node
		DirEntry parentEntry = getDirEntry(parent);
		Inode parentNode = inodes.get(parentEntry.inode);
		if (!(parentNode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentNode.data;
		// see if file already exists
		Path name = path.getFileName();
		DirEntry entry = parentDir.find(name);
		if (entry != null)
			throw new FilesystemException(Errno.FileExists);
		// create file
		FileData data = new FileData();
		Inode inode = new Inode(data, mode);
		parentDir.entries.add(new DirEntry(name, inode));
		return inode;
	}
	
	private Path parentOf(Path path) {
		Path parent = path.getParent();
		if (parent == null)
			parent = rootPath;
		return parent;
	}
	
	@Override
	protected void mknod(Path path, int mode, long rdev) throws FilesystemException {
		makeNode(path, mode, rdev);
	}
	
	@Override
	protected void unlink(Path path) throws FilesystemException {
		Path parent;
		DirEntry parentEntry;
		Inode parentInode;
		Path name;
		
		// get the from directory
		parent = parentOf(path);
		parentEntry = getDirEntry(parent);
		parentInode = inodes.get(parentEntry.inode);
		name = path.getFileName();
		if (!(parentInode.data instanceof Directory))
			throw new FilesystemException(Errno.NotADirectory);
		Directory parentDir = (Directory)parentInode.data;
		
		DirEntry entry = parentDir.find(name);
		Inode inode = inodes.get(entry.inode);
		if (inode.data instanceof Directory)
			throw new FilesystemException(Errno.IsADirectory);
		parentDir.remove(name);
	}
	
	@Override
	protected void statfs(Path path, StatVfs stat) throws FilesystemException {
		long blocksFree = Runtime.getRuntime().freeMemory() / FileData.BLOCKSIZE;
		stat.putBlocks(blockCount.get());
		stat.putBlocksAvail(blocksFree);
		stat.putBlocksFree(blocksFree);
		stat.putFiles(fileCount.get());
		stat.putFilesFree(Long.MAX_VALUE);
		stat.putNameMax(65536);
	}
	
	
}
