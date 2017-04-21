package org.apache.nifi.util.orc;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class FlowfileFileSystem extends FileSystem {
    private final OutputStream os;

    public FlowfileFileSystem(OutputStream os){
        this.os = os;
    }

    @Override
    public URI getUri() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        return new FSDataOutputStream(os, statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }
}
