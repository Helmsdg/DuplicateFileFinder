import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class DuplicateFileFinder{

	private List<String> searchDirectories;
	private Queue<File> filesToProcess;

	private Map<String, List<File>> duplicateMap;
	private Integer concurrentSearches;

	public DuplicateFileFinder(Integer concurrentSearches, List<String> searchDirectories){
		this.concurrentSearches = concurrentSearches;
		this.searchDirectories = searchDirectories;
		filesToProcess = new ArrayDeque<>();
		duplicateMap = new HashMap<String, List<File>>();
	}

	public Integer buildFileList(){
		for(String temp : searchDirectories){
            filesToProcess.addAll(fileBuilder(temp));
        }
        return filesToProcess.size();
	}

    private List<File> fileBuilder(String directory){
        List<File> toRet = new ArrayList<>();

        File dir = new File(directory);
        File[] levelFiles = dir.listFiles();
        if(levelFiles != null && levelFiles.length > 0){
            for(File temp : levelFiles){
                if(temp.isDirectory()){
                    toRet.addAll(fileBuilder(temp.getAbsolutePath()));
                }
                else{
                    toRet.add(temp);
                }
            }
        }
        return toRet;
    }

    public void buildDuplicateMap(){

        List<Thread> threadList = new ArrayList<>();
        for(int i = 0; i < concurrentSearches; i++){
            threadList.add(new FileDigester(duplicateMap, filesToProcess));
        }

        for(Thread temp : threadList){
            temp.start();
        }

        for(Thread temp : threadList){
            try {
                temp.join();
            }
            catch (InterruptedException err){
                err.printStackTrace();
            }
        }

    }

    public Map<String, List<File>> getDuplicateMap() {
        return duplicateMap;
    }

    private class FileDigester extends Thread {

        private Map<String, List<File>> duplicateMap;
        private Queue<File> filesToProcess;

        private MessageDigest messageDigest;

        public FileDigester(Map<String, List<File>> duplicateMap, Queue<File> filesToProcess){
            this.duplicateMap = duplicateMap;
            this.filesToProcess = filesToProcess;
            try {
                messageDigest = MessageDigest.getInstance("SHA1");
            }
            catch (NoSuchAlgorithmException err){
                err.printStackTrace();
            }
        }

        private File getFileToProcess(){
            File toRet = null;
            synchronized (filesToProcess){
                if(filesToProcess.size() > 0){
                    toRet = filesToProcess.remove();
                }
                if(filesToProcess.size() % 100 == 0){
                    System.out.println("Remaining: " + filesToProcess.size());
                }
            }

            return toRet;
        }

        private void addFileDigest(String digest, File file){
            synchronized (duplicateMap){
                if(!duplicateMap.containsKey(digest)){
                    duplicateMap.put(digest, new ArrayList<>());
                }
                duplicateMap.get(digest).add(file);
            }
        }

        private String calculateDigest(File file) throws FileNotFoundException, IOException{
            InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
            messageDigest.reset();
            final byte[] buffer = new byte[1024];
            for (int read = 0; (read = inputStream.read(buffer)) != -1; ) {
                messageDigest.update(buffer, 0, read);
            }
            inputStream.close();


            // Convert the byte to hex format
            Formatter formatter = new Formatter();
            for (final byte b : messageDigest.digest()) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        }

        @Override
        public void run() {
            File toProc = getFileToProcess();
            while(toProc != null){
                try {
                    addFileDigest(calculateDigest(toProc), toProc);
                }
                catch (IOException err){
                    err.printStackTrace();
                }
                toProc = getFileToProcess();
            }
        }
    }


	public static void main(String[] argv) throws FileNotFoundException{

        String outFile = argv[2];
        String reportFile = argv[1];
        PrintStream fullReport = new PrintStream(new File(reportFile));
        PrintStream ps = new PrintStream(new File(outFile));

		Integer concurrentFiles = Integer.parseInt(argv[0]);
		List<String> directories = new ArrayList<String>();
		for(String temp : argv){
			directories.add(temp);
		}
		//Remove first parameter from dir list
		directories.remove(0);
        directories.remove(0);

		DuplicateFileFinder duplicateFileFinder = new DuplicateFileFinder(concurrentFiles, directories);
        Integer fileCount = duplicateFileFinder.buildFileList();
        ps.println("File Count: " + fileCount);
        System.out.println("File Count: " + fileCount);
        duplicateFileFinder.buildDuplicateMap();

        Map<String, List<File>> reportList = duplicateFileFinder.getDuplicateMap();
        for(String key : reportList.keySet()){
            List<File> values = reportList.get(key);
            if(values.size() > 1){
                ps.println("Duplicates found for SHA1: " + key + " " + values.size() + " " + values.get(0).length() + "(" + values.get(0).length()*values.size() + ")");
                System.out.println("Duplicates found for SHA1: " + key + " " + values.size() + " " + values.get(0).length() + "(" + values.get(0).length() * values.size() + ")");
                for(File tempFile : values){
                   ps.println("\t" + tempFile.getAbsolutePath());
                }
            }
        }

        for(String key : reportList.keySet()) {
            List<File> values = reportList.get(key);
            fullReport.println(key);
            for(File tempFile : values){
                fullReport.println("\t" + tempFile.getAbsolutePath());
            }
        }
	}
}