import javax.naming.ldap.SortKey;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

/**
 *  This is just an example of how to use the provided client library. You are expected to customise this class and/or 
 *  develop other client classes to test your Controller and Dstores thoroughly. You are not expected to include client 
 *  code in your submission.
 */
public class ClientMain {
	
	public static void main(String[] args) throws Exception{
		
		final int cport = Integer.parseInt(args[0]);
		int timeout = Integer.parseInt(args[1]);
		
		// this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");
		
		// this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
		File uploadFolder = new File("folderToUpload");
		if (!uploadFolder.exists())
			throw new RuntimeException("folderToUpload folder does not exist");



		// REMOTE VALIDATION TEST 1

//		Client client = new Client(cport,timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
//		client.connect();
//		testList(client);
//
//		Client client1 = new Client(cport,timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
//		client1.connect();
//		testList(client1);
//		// removeValTest1(client,uploadFolder);




		// TEST THE CONCURRENCY
		for ( int i = 0 ; i < 1 ;i++) {
			System.out.println("OPENING CLIENT " + i);
			int finalI = i;
			new Thread(() -> {
				try {
					// OPEN CLIENT I
					Client client = new Client(cport,timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
					client.connect();
					// START LOOPING LIST FOR INFINITE
					testClientConnection(client, finalI);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}).start();
			 // testClientConnection(client,i);

		}








		/// My tests :
//
//		testStore(client, uploadFolder);
//
//		testLoad(client);
//
//		testRemove(client);
//
//		testList(client);
		// myTest1(client, uploadFolder);






		/// Tested provided by University

		//launch a single client
		// testClient(cport, timeout, downloadFolder, uploadFolder);
		
		// launch a number of concurrent clients, each doing the same operations
//		for (int i = 0; i < 2; i++) {
//			new Thread() {
//				public void run() {
//					test2Client(cport, timeout, downloadFolder, uploadFolder);
//				}
//			}.start();
//		}







	}

	private static void testClientConnection(Client client,int i) throws InterruptedException {

		// LOOP LIST EVERY ONE SECOND
		// for(;;) {
			Thread.sleep(1000);
			client.send("LIST");
		// }

		// System.out.println("Client disconnected");

	}

	private static void removeValTest1(Client client, File uploadFolder) throws IOException {

		client.list();
		client.store(new File(uploadFolder + "/" + "small_file.jpg"));
		client.store(new File(uploadFolder + "/" + "small_file.jpg"));
		client.list();
		client.load("small_file.jpg");

	}

	private static void myTest1(Client client,File uploadFolder) throws IOException {

		client.list();
		client.store(new File(uploadFolder + "/" + "file1.txt"));

	}

	private static void testList(Client client) throws IOException {
		client.list();
	}


	private static void testStore(Client client, File uploadFolder) throws IOException {

		client.list();
		client.store(new File(uploadFolder + "/" + "file1.txt"));
		// client.wrongStore("file.txt",new byte[100]);


	}

	private static void testLoad(Client client) throws IOException {

		client.load( "file1.txt");
		// client.wrongLoad("file1.txt",3);

	}


	private static void testRemove(Client client) throws IOException {

		client.remove("file1.txt");

	}



	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());
			
			File fileList[] = uploadFolder.listFiles();
			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {					
					client.store(fileToStore);
				} catch (Exception e) {
					System.out.println("Error storing file " + fileToStore);
					e.printStackTrace();
				}
			}
			
			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}
			
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}
	
	public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;
		
		try {
			
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
		
			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }				
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();
		
		System.out.println("Ok, " + list.length + " files:");
		int i = 0; 
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);

		return list;
	}
	
}
