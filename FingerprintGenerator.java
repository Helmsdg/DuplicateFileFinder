import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Scanner;

/**
 * Created by david on 7/8/15.
 */
public class FingerprintGenerator {

    private String inputFile;
    private Integer concurrentSearches;
    private Map<String, List<File>> fileMap;

    private Map<File, String> fingerprintMap;
    private Queue<File> fileQueue;

    public FingerprintGenerator(Integer concurrentSearches, String inputFile){
        this.concurrentSearches = concurrentSearches;
        this.inputFile = inputFile;
        this.fileMap = new HashMap<String, List<File>>();

        this.fingerprintMap = new HashMap<File, String>();
        this.fileQueue = new ArrayDeque<>();
    }

    public Integer buildProcessList() throws FileNotFoundException{

        Scanner scanner = new Scanner(new File(inputFile));
        String line = scanner.nextLine();
        while(scanner.hasNext()){
            String key = line;
            fileMap.put(key.trim(), new ArrayList<File>());
            //Process file list
            line = scanner.nextLine();
            while(line.contains("\t")){
                fileMap.get(key).add(new File(line.trim()));
                try {
                    line = scanner.nextLine();
                }
                catch (NoSuchElementException err){
                    break;
                }
            }
        }

        //Add only a single copy of each hash
        for(String key : fileMap.keySet()){
            fileQueue.add(fileMap.get(key).get(0));
        }

        return fileQueue.size();

    }

    public void processFingerprints(){
        List<Thread> threadList = new ArrayList<>();
        for(int i = 0; i < concurrentSearches; i++){
            threadList.add(new FileFingerPrinter(fingerprintMap, fileQueue));
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

    public Map<File, String> getFingerprintMap() {
        return fingerprintMap;
    }

    public Map<String, List<File>> getFileMap() {
        return fileMap;
    }

    public static void main(String[] argv) throws FileNotFoundException {

        String outFile = argv[2];
        String inFile = argv[1];
        PrintStream ps = new PrintStream(new File(outFile));

        Integer concurrentFiles = Integer.parseInt(argv[0]);

        FingerprintGenerator fingerprintGenerator = new FingerprintGenerator(concurrentFiles, inFile);
        Integer fileCount = fingerprintGenerator.buildProcessList();
        ps.println("File Count: " + fileCount);
        System.out.println("File Count: " + fileCount);
        fingerprintGenerator.processFingerprints();

        Map<String, List<File>> fileMap = fingerprintGenerator.getFileMap();

        Map<File, String> fingerprintMap = fingerprintGenerator.getFingerprintMap();

        for(String key : fileMap.keySet()){
            List<File> values = fileMap.get(key);
            ps.println(key);
            ps.println(fingerprintMap.get(values.get(0)).trim().replace("\n", "\t").replace("\r", "\t"));
        }
        ps.close();


    }

    private class FileFingerPrinter extends Thread {

        private Map<File, String> fingerprintMap;
        private Queue<File> filesToProcess;


        public FileFingerPrinter(Map<File, String> fingerprintMap, Queue<File> filesToProcess){
            this.fingerprintMap = fingerprintMap;
            this.filesToProcess = filesToProcess;
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

        private void addFileDigest(String fingerprint, File file){
            synchronized (fingerprintMap){
                fingerprintMap.put(file, fingerprint);
            }
        }

        private String calculateFingerprint(File file) throws IOException, InterruptedException {
            long threadId = Thread.currentThread().getId();
            System.out.println("Thread # " + threadId + " is doing this task");
            Runtime r = Runtime.getRuntime();
            System.out.println("1");
            System.out.println("fpcalc '" + file.getAbsolutePath() + "'");
            Process p = r.exec("fpcalc '" + file.getAbsolutePath() + "'");
            System.out.println("2");
            p.wait();
            BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            String collector = "";
            System.out.println("3");


            while ((line = b.readLine()) != null) {
                collector += line;
                System.out.println(line);
            }

            System.out.println("4");

            b.close();

            System.out.println("5");


            return collector;
        }

        @Override
        public void run() {
            File toProc = getFileToProcess();
            while(toProc != null){
                try {
                    addFileDigest(calculateFingerprint(toProc), toProc);
                }
                catch (IOException | InterruptedException err){
                    err.printStackTrace();
                }
                toProc = getFileToProcess();
            }
        }
    }
}
