package org.adrianwalker.ftpserver.filesystem;

import static java.io.File.separator;

import org.adrianwalker.cassandra.filesystem.controller.FileSystemController;
import org.adrianwalker.cassandra.filesystem.entity.File;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class CassandraFileSystemView implements FileSystemView {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFileSystemView.class);

  private final User user;
  private final FileSystemController controller;

  private final String homeDirectory;
  private String workingDirectory;

  public CassandraFileSystemView(final User user, final FileSystemController controller) {

    LOGGER.debug("user = {}, controller = {}", user, controller);

    if (null == user) {
      throw new IllegalArgumentException("user is null");
    }

    if (null == controller) {
      throw new IllegalArgumentException("controller is null");
    }

    this.user = user;
    this.controller = controller;

    this.homeDirectory = user.getHomeDirectory();
    this.workingDirectory = homeDirectory;
  }

  @Override
  public FtpFile getHomeDirectory() throws FtpException {

    LOGGER.debug("homeDirectory = {}", homeDirectory);

    FtpFile file = getFile(homeDirectory);

    if (!file.doesExist()) {
      file = createDirectory(homeDirectory);
    }

    return file;
  }

  @Override
  public FtpFile getWorkingDirectory() throws FtpException {

    LOGGER.debug("workingDirectory = {}", workingDirectory);

    FtpFile file = getFile(workingDirectory);

    if (!file.doesExist()) {
      file = createDirectory(workingDirectory);
    }

    return file;
  }

  @Override
  public boolean changeWorkingDirectory(final String workingDirectory) throws FtpException {

    LOGGER.debug("workingDirectory = {}", workingDirectory);

    FtpFile file = getFile(workingDirectory);
    boolean exists = file.doesExist();

    if (exists) {
      this.workingDirectory = file.getAbsolutePath();
    }

    return exists;
  }

  @Override
  public FtpFile getFile(final String name) throws FtpException {

    LOGGER.debug("name = {}", name);

    if (null == name) {
      throw new IllegalArgumentException("name is null");
    }

    String path = normalize(name);
    File file = controller.getFile(path);

    return new CassandraFtpFile(user, path, file, controller);
  }

  @Override
  public boolean isRandomAccessible() throws FtpException {

    return false;
  }

  @Override
  public void dispose() {
  }

  private String normalize(final String name) {

    LOGGER.debug("name = {}", name);

    Path path;
    if (name.startsWith(separator)) {
      path = Paths.get(name);
    } else {
      path = Paths.get(workingDirectory, name);
    }

    String normalizedName = path
            .normalize()
            .toString();

    LOGGER.debug("normalizedName = {}", normalizedName);

    return normalizedName;
  }

  private FtpFile createDirectory(final String path) {

    File directory = new File();
    directory.setName(Paths.get(path).getFileName().toString());
    directory.setDirectory(true);
    directory.setOwner(user.getName());
    directory.setGroup(user.getName());
    directory.setModified(System.currentTimeMillis());

    controller.saveFile(path, directory);

    return new CassandraFtpFile(user, path, directory, controller);
  }
}
