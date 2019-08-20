package com.nikey.fix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author dyf
 * @date 2016年12月05日 上午11:00:00
 * 
 * 扫描文件夹下所有文件，读取文件的内容
 */
public class ScanFile
{

    public static void main(String[] args) throws Exception
    {
        File root = new File("/home/dyf/文档/logs");
        showAllFiles(root);
    }
    
    final static void showAllFiles(File dir) throws Exception{
        File[] fs = dir.listFiles();
        for(int i=0; i<fs.length; i++){
//            System.out.println(fs[i].getAbsolutePath());
            Map<String,Integer> resultMap = new HashMap<String,Integer>();
            try{
                BufferedReader br = new BufferedReader(new FileReader(fs[i]));//构造一个BufferedReader类来读取文件
                String s = null;
                while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                    String add= s.substring(0, 6);
                    if(add.equals("183.63")){
                        String str[] = s.split("\"");
                        if(str[1].indexOf("RealTimeController/EventRtController/realTimeEvents") == -1
                                &&str[1].indexOf("RealTimeController/EventRtController/realTimeTempEvents") == -1){//过滤不要的行
                            int index = 0;
                            try
                            {
                                String str1=str[1];
                                String substr[]=str1.split("\\?");
                                System.out.println(substr[0]);
                                if(resultMap.get(substr[0])!=null){
                                    index=resultMap.get(substr[0]);
                                }
                                index++;
                                resultMap.put(substr[0], index);
                            } catch (Exception e)
                            {
                                if(resultMap.get(str[1])!=null){
                                    index=resultMap.get(str[1]);
                                }
                                index++;
                                resultMap.put(str[1], index);
                            }
                        }
                    }
                }
                br.close();
                for (Entry<String, Integer> entry : resultMap.entrySet()) {  
                    String str = entry.getKey()+"  :  "+entry.getValue();
                    terminalSettingsBackup2(fs[i].getName(),str);//将新的内容写入到新的文档中
                }  
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
    public static void terminalSettingsBackup2(String add,String backupstr) {
        try{
            String path = "/home/dyf/文档/logrecord/"+add;
            
            File file = new File(path);
            if(!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if(!file.exists()) { 
                file.createNewFile();
            }
            if(file.exists()) {
                FileWriter writer = new FileWriter(file, true);
                writer.write(backupstr + "\n");
                writer.flush();
                writer.close();
                
            }
        }catch(Exception e){}
    }
}
