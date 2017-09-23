package org.adrianwalker.ftpserver.filesystem;

import static java.util.stream.Collectors.toList;

import org.adrianwalker.cassandra.filesystem.controller.FileSystemController;
import org.adrianwalker.cassandra.filesystem.entity.File;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;

public final class CassandraFtpFile implements FtpFile {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFtpFile.class);

  private final User user;
  private final String path;
  private File file;
  private final FileSystemController controller;

  public CassandraFtpFile(
          final User user,
          final String path,
          final File file,
          final FileSystemController controller) {

    LOGGER.debug("user = {}, path = {}, file = {}, controller = {}", user, path, file, controller);

    if (null == user) {
      throw new IllegalArgumentException("user is null");
    }

    if (null == path) {
      throw new IllegalArgumentException("path is null");
    }

    if (null == controller) {
      throw new IllegalArgumentException("controller is null");
    }

    this.user = user;
    this.path = path;
    this.file = file;
    this.controller = controller;
  }

  @Override
  public String getAbsolutePath() {

    LOGGER.debug("path = {}", path);

    return path;
  }

  @Override
  public String getName() {

    String name = file.getName();
    LOGGER.debug("name = {}", name);

    return name;
  }

  @Override
  public boolean isHidden() {

    boolean hidden = file.isHidden();
    LOGGER.debug("hidden = {}", hidden);

    return hidden;
  }

  @Override
  public boolean isDirectory() {

    boolean directory = file.isDirectory();
    LOGGER.debug("directory = {}", directory);

    return directory;
  }

  @Override
  public boolean isFile() {

    boolean file = !isDirectory();
    LOGGER.debug("file = {}", file);

    return file;
  }

  @Override
  public boolean doesExist() {

    boolean exists = file != null;
    LOGGER.debug("exists = {}", exists);

    return exists;
  }

  @Override
  public boolean isReadable() {

    boolean readable = doesExist();
    LOGGER.debug("readable = {}", readable);

    return readable;
  }

  @Override
  public boolean isWritable() {

    boolean writable = path.startsWith(user.getHomeDirectory());
    LOGGER.debug("writable = {}", writable);

    return writable;
  }

  @Override
  public boolean isRemovable() {

    boolean removable = doesExist() && isWritable();
    LOGGER.debug("removable = {}", removable);

    return removable;
  }

  @Override
  public String getOwnerName() {

    String owner = file.getOwner();
    LOGGER.debug("owner = {}", owner);

    return owner;
  }

  @Override
  public String getGroupName() {

    String group = file.getGroup();
    LOGGER.debug("group = {}", group);

    return group;
  }

  @Override
  public int getLinkCount() {

    int linkCount = file.isDirectory() ? 2 : 1;
    LOGGER.debug("linkCount = {}", linkCount);

    return linkCount;
  }

  @Override
  public long getLastModified() {

    long lastModified = file.getModified();
    LOGGER.debug("lastModified = {}", lastModified);

    return lastModified;
  }

  @Override
  public boolean setLastModified(final long lastModified) {

    LOGGER.debug("lastModified = {}", lastModified);
    file.setModified(lastModified);

    return controller.saveFile(path, file);
  }

  @Override
  public long getSize() {

    long size = file.getSize();
    LOGGER.debug("size = {}", size);

    return size;
  }

  @Override
  public Object getPhysicalFile() {

    LOGGER.debug("file = {}", file);

    return file;
  }

  @Override
  public boolean mkdir() {

    LOGGER.debug("path = {}", path);

    File directory = new File();
    directory.setName(Paths.get(path).getFileName().toString());
    directory.setDirectory(true);
    directory.setOwner(user.getName());
    directory.setGroup(user.getName());

    return controller.saveFile(path, directory);
  }

  @Override
  public boolean delete() {

    LOGGER.debug("path = {}", path);

    return controller.deleteFile(path);
  }

  @Override
  public boolean move(final FtpFile ftpFile) {

    LOGGER.debug("ftpFile = {}", ftpFile);

    if (null == ftpFile) {
      throw new IllegalArgumentException("ftpFile is null");
    }

    return controller.moveFile(path, ftpFile.getAbsolutePath());
  }

  @Override
  public List<CassandraFtpFile> listFiles() {

    LOGGER.debug("path = {}", path);

    return controller.listFiles(path)
            .stream().map(file -> new CassandraFtpFile(
            user, Paths.get(path, file.getName()).toString(), file, controller))
            .collect(toList());
  }

  @Override
  public OutputStream createOutputStream(final long offset) throws IOException {

    LOGGER.debug("offset = {}", offset);

    if (offset != 0) {
      throw new IllegalArgumentException("zero offset unsupported");
    }

    if (null == file) {
      file = new File();
      file.setName(Paths.get(path).getFileName().toString());
      file.setDirectory(false);
      file.setOwner(user.getName());
      file.setGroup(user.getName());
      file.setModified(System.currentTimeMillis());

      controller.saveFile(path, file);
    }

    return new BufferedOutputStream(controller.createOutputStream(file));
  }

  @Override
  public InputStream createInputStream(final long offset) throws IOException {

    LOGGER.debug("offset = {}", offset);

    if (offset != 0) {
      throw new IllegalArgumentException("zero offset unsupported");
    }

    return new BufferedInputStream(controller.createInputStream(file));
  }
}
