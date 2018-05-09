package com.gzfns.obdpps.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static Future<Boolean> writeToFile(String rawData, Vertx vertx, String dir, String fileName) {
        Future<Boolean> result = Future.future();

        String existFileName = getOrCreateFileName(vertx, dir, fileName);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss---");
        String currDateTime = dateFormat.format(new Date());

        OpenOptions options = new OpenOptions().setAppend(true);//.setSync(true);
        FileSystem fileSystem = vertx.fileSystem();
        fileSystem.open(existFileName, options, openRes -> {
            if(openRes.succeeded()){
                AsyncFile rawDataFile = openRes.result();
                Buffer buffer = Buffer.buffer(currDateTime + rawData + "\r\n", "ASCII");
                rawDataFile.write(buffer).flush(p -> {
                    if(p.succeeded()){
                        result.complete(true);
                    }
                    else {
                        logger.error("Failed flush message.", p.cause());
                        result.fail(p.cause());
                    }

                    rawDataFile.close(closeRes -> {
                        if(closeRes.succeeded()){
                            logger.trace("Async raw data file closed successfully.");
                        }
                        else {
                            logger.error("Failed close async raw data file.");
                        }
                    });
                });
            }
            else {
                logger.error("Failed open raw data file.", openRes.cause());
                result.fail(openRes.cause());
            }
        });


        return result;
    }

    public static String getOrCreateFileName(Vertx vertx, String dir, String fileName) {
        String returnFileName = "";
        Boolean dirExists = vertx.fileSystem().existsBlocking(dir);
        if(dirExists){
            logger.trace("dir has exists, dir: " + dir);

            try {
                returnFileName = createFile(vertx, fileName);
            }
            catch (Exception ex){
                logger.error("Failed create file: " + fileName, ex);
            }
        }
        else {
            try {
                vertx.fileSystem().mkdirsBlocking(dir, "rwxr-x---");
                logger.trace("mkdirs successfully.");

                returnFileName = createFile(vertx, fileName);
            }
            catch (Exception e) {
                logger.error("Failed mkdirs: " + dir + ", Or Failed create file: " + fileName, e);
            }
        }

        return returnFileName;
    }

    public static String createFile(Vertx vertx, String fileName) throws Exception{
        Boolean fileExists = vertx.fileSystem().existsBlocking(fileName);
        if(!fileExists){
            logger.trace("file not exists. will create file. fileName: " + fileName);
            vertx.fileSystem().createFileBlocking(fileName, "rwxr-x---");
        }

        return fileName;
    }

}
