package com.test.jhh;

import java.io.BufferedReader;  
import java.io.File;  
import java.io.FileWriter;  
import java.io.InputStream;  
import java.io.InputStreamReader;  
import java.util.Collections;  
import java.util.Comparator;  
import java.util.LinkedList;  
import java.util.UUID;  
  
public class ProjectProc {  
  
    class FileComp implements Comparator<File> {  

        public int compare(File o1, File o2) {  
  
            return o1.getPath().compareTo(o2.getPath());  
        }  
    }  
  
    static String[] g_ayExt = { "cpp", "h", "cxx", "c", "java", "hpp", "hxx",  
            "cs", "php" };  
    static LinkedList<String> g_llExt = new LinkedList<String>();  
  
    public static String getConfigFileName() {  
        File f = new File("myconfig.conf");  
        String path = f.getAbsolutePath();  
        return path;  
    }  
  
    private boolean isSourceFile(String path) {  
  
        if (g_llExt.size() == 0) {  
            for (String e : g_ayExt) {  
                g_llExt.add(e);  
            }  
        }  
  
        int idx = path.lastIndexOf('.');  
        if (idx <= 0) {  
            return false;  
        }  
        String ext = path.substring(idx + 1);  
        if (g_llExt.contains(ext)) {  
            return true;  
        } else {  
            return false;  
        }  
    }  
  
    public void process(String path) throws Exception {  
        BufferedReader br = null;  
        FileWriter fw = null;  
        try {  
  
            String full = "";  
            InputStream templ = getClass().getResourceAsStream(  
                    "template.vcproj.txt");  
            br = new BufferedReader(new InputStreamReader(templ));  
            while (br.ready()) {  
                full += br.readLine() + "\r\n";  
            }  
  
            File f = new File(path);  
            path = f.getAbsolutePath();  
            if (!path.endsWith(File.separator)) {  
                path = path + File.separator;  
            }  
  
            String projName = path.substring(  
                    path.lastIndexOf(File.separator, path.length() - 2) + 1,  
                    path.length() - 1);  
  
            // key function  
            String result = SearchDir(path);  
            result = result.replace(path, "");  
  
            full = full.replace("{{NAME}}", projName);  
            full = full.replace("{{GUID}}", "{" + UUID.randomUUID().toString()  
                    + "}");  
            full = full.replace("{{FILES}}", result);  
  
            String resFile = path + projName + ".vcproj";  
            fw = new FileWriter(resFile);  
            fw.write(full.toCharArray());  
            fw.flush();  
  
        } catch (Exception e) {  
            e.printStackTrace();  
            throw e;  
        } finally {  
            if (br != null) {  
                br.close();  
            }  
            if (fw != null) {  
                fw.close();  
            }  
        }  
    }  
  
    /** 
     * @param path 
     *            ���·�� 
     * @return string 
     */  
    private String SearchDir(String path) {  
        if (!path.endsWith(File.separator)) {  
            path = path + File.separator;  
        }  
        String name = path.substring(  
                path.lastIndexOf(File.separator, path.length() - 2) + 1,  
                path.length() - 1);  
        String result = String.format("<Filter Name=\"%s\" Filter=\"\">", name);  
        File dir = new File(path);  
        File[] files = dir.listFiles();  
        if (files == null) {  
            result += "</Filter>";  
            return result;  
        }  
  
        LinkedList<File> list = new LinkedList<File>();  
        for (File file : files) {  
            list.add(file);  
        }  
        FileComp c = new FileComp();  
        Collections.sort(list, c);  
  
        for (File file : list) {  
  
            if (file.isHidden()) {  
                // do nothing, ignore  
                System.out.println("file is hidden: " + file.getPath());  
                continue;  
            }  
            if (file.getName().startsWith(".")) {  
                // do nothing, ignore  
                System.out.println("file is start with '.': " + file.getPath());  
                continue;  
            }  
  
            if (file.isDirectory()) {  
                String sub = SearchDir(file.getPath());  
                result += sub;  
            } else {  
                if (isSourceFile(file.getPath())) {  
                    String sub = String.format(  
                            "<File RelativePath=\"%s\"></File>\r\n",  
                            file.getPath());  
                    result += sub;  
                }  
            }  
        }  
        result += "</Filter>";  
        return result;  
    }  
  
}  