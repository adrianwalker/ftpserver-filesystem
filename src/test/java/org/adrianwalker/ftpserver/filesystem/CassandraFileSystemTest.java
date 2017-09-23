package org.adrianwalker.ftpserver.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.adrianwalker.cassandra.filesystem.controller.FileSystemController;
import org.adrianwalker.cassandra.filesystem.entity.File;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;

public class CassandraFileSystemTest {

  private static final long TIMEOUT = 30_000L;

  private static final String HOST = "127.0.0.1";
  private static final int PORT = 9142;
  private static final String KEYSPACE = "filesystem";
  private static final String CQL = "filesystem.cql";

  private static final String USERNAME = "testuser";
  private static final String HOME_DIRECTORY = "/" + USERNAME;
  private static final String OWNER = USERNAME;
  private static final String GROUP = USERNAME;
  private static final boolean DIRECTORY = true;
  private static final boolean NOT_DIRECTORY = !DIRECTORY;
  private static final boolean NOT_HIDDEN = false;

  private static final String[] CLEAN_UP_CQL = {
    "TRUNCATE parent_path",
    "TRUNCATE path",
    "TRUNCATE file",
    "TRUNCATE chunk"
  };

  private static Session session;
  private static FileSystemController controller;

  public CassandraFileSystemTest() {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {

    EmbeddedCassandraServerHelper.startEmbeddedCassandra(TIMEOUT);

    Cluster cluster = new Cluster.Builder()
            .addContactPoints(HOST)
            .withPort(PORT)
            .build();
    session = cluster.connect();

    CQLDataLoader dataLoader = new CQLDataLoader(session);
    dataLoader.load(new ClassPathCQLDataSet(CQL, KEYSPACE));

    controller = new FileSystemController(session);
  }

  @AfterClass
  public static void afterClass() {

    session.close();
  }

  @After
  public void after() {

    for (String cql : CLEAN_UP_CQL) {
      session.execute(cql);
    }
  }

  @Test
  public void testGetHomeDirectory() throws FtpException {

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile home = view.getHomeDirectory();

    assertNotNull(home);
    assertEquals(HOME_DIRECTORY, home.getAbsolutePath());
  }

  @Test
  public void testGetWorkingDirectory() throws FtpException {

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile workingDirectory = view.getWorkingDirectory();
    assertNotNull(workingDirectory);
    assertEquals(HOME_DIRECTORY, workingDirectory.getAbsolutePath());
  }

  @Test
  public void testChangeWorkingDirectory() throws FtpException {

    String directoryName = "testdir";
    String path = Paths.get(HOME_DIRECTORY, directoryName).toString();
    controller.saveFile(path, newDirectory(directoryName));

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    boolean changeWorkingDirectory = view.changeWorkingDirectory(path);
    assertTrue(changeWorkingDirectory);

    FtpFile workingDirectory = view.getWorkingDirectory();
    assertNotNull(workingDirectory);
    assertEquals(path, workingDirectory.getAbsolutePath());
  }

  @Test
  public void testMakeDirectory() throws FtpException {

    String directoryName = "testdir";

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile directory = view.getFile(directoryName);
    assertNotNull(directory);
    assertFalse(directory.doesExist());

    directory.mkdir();

    directory = view.getFile(directoryName);
    assertNotNull(directory);
    assertTrue(directory.doesExist());

    String path = Paths.get(HOME_DIRECTORY, directoryName).toString();
    assertEquals(path, directory.getAbsolutePath());
  }

  @Test
  public void testGetFile() throws FtpException, IOException {

    String fileName = "testfile";
    String path = Paths.get(HOME_DIRECTORY, fileName).toString();
    controller.saveFile(path, newFile(fileName));

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile file = view.getFile(fileName);
    assertNotNull(file);
    assertTrue(file.doesExist());
    assertEquals(fileName, file.getName());
    assertEquals(path, file.getAbsolutePath());
    assertFalse(file.isDirectory());
    assertTrue(file.isFile());
    assertFalse(file.isHidden());
    assertTrue(file.isReadable());
    assertTrue(file.isWritable());
    assertTrue(file.isRemovable());
    assertTrue(file.isWritable());
    assertEquals(GROUP, file.getGroupName());
    assertEquals(OWNER, file.getOwnerName());
    assertEquals(0, file.getSize());
    assertTrue(file.getLastModified() > 0);
    assertTrue(file.getLastModified() < System.currentTimeMillis());
    assertEquals(1, file.getLinkCount());
  }

  @Test
  public void testGetPhysicalFile() throws FtpException, IOException {

    String fileName = "testfile";
    String path = Paths.get(HOME_DIRECTORY, fileName).toString();
    controller.saveFile(path, newFile(fileName));

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile file = view.getFile(fileName);
    Object physicalFile = file.getPhysicalFile();

    assertNotNull(physicalFile);
    assertTrue(physicalFile instanceof File);
  }

  @Test
  public void testDeleteFile() throws FtpException, IOException {

    String fileName = "testfile";
    String path = Paths.get(HOME_DIRECTORY, fileName).toString();
    controller.saveFile(path, newFile(fileName));

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile file = view.getFile(fileName);
    assertNotNull(file);
    assertTrue(file.doesExist());

    file.delete();

    file = view.getFile(fileName);
    assertNotNull(file);
    assertFalse(file.doesExist());
  }

  @Test
  public void testMoveFile() throws FtpException {

    String fromFileName = "testfile";
    String fromFilePath = Paths.get(HOME_DIRECTORY, fromFileName).toString();
    controller.saveFile(fromFilePath, newFile(fromFileName));

    String directoryName = "testdir";
    String directoryPath = Paths.get(HOME_DIRECTORY, fromFileName).toString();
    controller.saveFile(directoryPath, newDirectory(directoryName));

    String toFileName = "testfile";
    String toFilePath = Paths.get(HOME_DIRECTORY, directoryName, toFileName).toString();

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile fromFile = view.getFile(fromFilePath);
    assertNotNull(fromFile);
    assertTrue(fromFile.doesExist());

    FtpFile toFile = view.getFile(toFilePath);
    assertFalse(toFile.doesExist());

    fromFile.move(toFile);

    fromFile = view.getFile(fromFilePath);
    assertNotNull(fromFile);
    assertFalse(fromFile.doesExist());

    toFile = view.getFile(toFilePath);
    assertNotNull(toFile);
    assertTrue(toFile.doesExist());
  }

  @Test
  public void testListFiles() throws FtpException, IOException {

    String fileName = "testfile";

    int count = 3;

    for (int i = 0; i < count; i++) {
      String path = Paths.get(HOME_DIRECTORY, fileName + i).toString();
      controller.saveFile(path, newFile(fileName + i));
    }

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile directory = view.getFile(HOME_DIRECTORY);
    List<? extends FtpFile> files = directory.listFiles();
    files.sort((FtpFile f1, FtpFile f2) -> f1.getName().compareTo(f2.getName()));

    assertNotNull(files);
    assertEquals(count, files.size());

    for (int i = 0; i < count; i++) {

      FtpFile file = files.get(i);

      assertNotNull(file);
      assertEquals(fileName + i, file.getName());
    }
  }

  @Test
  public void testModified() throws FtpException, IOException {

    String fileName = "testfile";
    String path = Paths.get(HOME_DIRECTORY, fileName).toString();
    controller.saveFile(path, newFile(fileName));

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    FtpFile file = view.getFile(fileName);
    long lastModified = file.getLastModified();
    assertTrue(lastModified > 0);

    long now = System.currentTimeMillis();
    file.setLastModified(now);

    file = view.getFile(fileName);
    lastModified = file.getLastModified();
    assertEquals(now, lastModified);
  }

  @Test
  public void testIOStreams() throws FtpException, IOException {

    CassandraFileSystemFactory factory = new CassandraFileSystemFactory(controller);

    User user = newUser(USERNAME);
    FileSystemView view = factory.createFileSystemView(user);

    String fileName = "testfile";
    FtpFile file = view.getFile(fileName);

    int writeBytes = (2 * 1024 * 1024) + 1;
    long offset = 0L;

    try (OutputStream os = file.createOutputStream(offset)) {

      for (int i = 0; i < writeBytes; i++) {
        os.write((byte) 'x');
      }
    }

    file = view.getFile(fileName);
    assertEquals(writeBytes, file.getSize());

    int bytesRead = 0;

    try (InputStream in = file.createInputStream(offset)) {

      int i;
      while ((i = in.read()) != -1) {

        assertEquals((byte) 'x', i);
        bytesRead += 1;
      }
    }

    assertEquals(writeBytes, bytesRead);
  }

  private User newUser(final String name) {

    BaseUser user = new BaseUser();

    user.setEnabled(true);
    user.setHomeDirectory("/" + name);
    user.setName(name);
    user.setPassword("password");

    return user;
  }

  private File newFile(final String name) {

    File file = new File();

    file.setName(name);
    file.setDirectory(NOT_DIRECTORY);
    file.setOwner(OWNER);
    file.setGroup(GROUP);
    file.setHidden(NOT_HIDDEN);
    file.setModified(System.currentTimeMillis());

    return file;
  }

  private File newDirectory(final String name) {

    File directory = new File();

    directory.setName(name);
    directory.setDirectory(DIRECTORY);
    directory.setOwner(OWNER);
    directory.setGroup(GROUP);
    directory.setHidden(NOT_HIDDEN);
    directory.setModified(System.currentTimeMillis());

    return directory;
  }
}
