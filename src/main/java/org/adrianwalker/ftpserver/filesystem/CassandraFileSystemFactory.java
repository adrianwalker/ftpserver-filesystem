package org.adrianwalker.ftpserver.filesystem;

import org.adrianwalker.cassandra.filesystem.controller.FileSystemController;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraFileSystemFactory implements FileSystemFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraFileSystemFactory.class);

  private final FileSystemController controller;

  public CassandraFileSystemFactory(final FileSystemController controller) {

    LOGGER.debug("controller = {}", controller);

    if (null == controller) {
      throw new IllegalArgumentException("controller is null");
    }

    this.controller = controller;
  }

  @Override
  public FileSystemView createFileSystemView(final User user) throws FtpException {

    LOGGER.debug("user = {}", user);

    if (null == user) {
      throw new IllegalArgumentException("user is null");
    }

    return new CassandraFileSystemView(user, controller);
  }
}
