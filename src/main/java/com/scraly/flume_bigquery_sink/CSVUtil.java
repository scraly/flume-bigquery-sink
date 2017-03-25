package com.scraly.flume_bigquery_sink;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Scanner;

/**
 * When adding a new version, don't forget to update the VERSION_LATEST constant
 * 
 */
public class CSVUtil {
    
    public static final String VERSION_1 = "v1";
    protected static final LogField[] version1 = new LogField[] {
        LogField.lv,
        LogField.tid,
        LogField.id,
        LogField.cdt,
        LogField.txt
    };
    
    public static final String VERSION_LATEST = VERSION_1;
    
    private CSVWriter csvWriter;
    private CSVParser csvParser;
    
    public CSVUtil() {
        this.csvWriter = new CSVWriter();
        this.csvParser = new CSVParser();
    }
    
    public CSVUtil(char escapechar) {
        this.csvWriter = new CSVWriter(CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, escapechar);
        this.csvParser = new CSVParser();
    }
    
    private LogField[] getVersion(String v) {
        LogField[] version = version1;
//        if (VERSION_2.equals(v)) {
//        	version = version2;
//        } 
        return version;
    }
    
    /**
     * Write the CSV line according to the fields order specified by the log version.
     * @param fields
     * @return an ordered csv line
     */
    public String write(EnumMap<LogField, String> fields) {
        LogField[] version = getVersion(fields.get(LogField.lv));
        String[] res = new String[version.length];
        for (int i=0; i<version.length; i++) {
            res[i] = fields.get(version[i]);
        }
        return csvWriter.write(res);
    }

    /**
     * When parsing the log, we expect the first field to be the log version ("v1" or greater)
     * @param log
     * @return
     * @throws IOException
     */
    public EnumMap<LogField, String> parse(String log) throws IOException {
        final String[] fields = csvParser.parseLine(log);
        LogField[] version = getVersion(fields[0]);
        EnumMap<LogField, String> res = new EnumMap<LogField, String>(LogField.class);
        for (int i=0; i<fields.length; i++) {
            res.put(version[i], fields[i]);
        }
        return res;
    }

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
//        EnumMap<LogField, String> res = new EnumMap<LogField, String>(LogField.class);
//        res.put(LogField.lv, VERSION_1);
//        res.put(LogField.cdt, "129832303203");
//        res.put(LogField.txt, "Women in tech!");
//        String log = new CSVUtil().write(res);
//        System.out.println(log);
//        EnumMap<LogField, String> res2 = new CSVUtil().parse(log);
//        for (Entry<LogField, String> e : res2.entrySet()) {
//            System.out.println("res2{'"+e.getKey()+"'}=\""+e.getValue()+"\"");
//        }

		File cl = new File("/tmp/test.txt");
		Scanner scanner = null;
		try {
			scanner = new Scanner(cl, "UTF-8");
			while (scanner.hasNextLine()) {
				String[] line = new String[] {scanner.nextLine()};
				String res = new CSVUtil().csvWriter.write(line);
				System.out.println(line[0]);
				System.out.println(res);
				String[] par = new CSVUtil().csvParser.parseLine(res);
				System.out.println(par[0]);
//				String[] fields = new CSVUtil().csvParser.parseLine(line);
//				LogField[] version = new CSVUtil().getVersion(fields[0]);
//				for (int i=0; i<fields.length; i++) {
//					System.out.println(i+"/"+version[i]+"="+fields[i]);
//				}
			}
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (scanner != null)
				scanner.close();
		}

    }

}
