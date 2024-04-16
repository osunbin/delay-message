package com.bin.delay.file.log;

import com.bin.delay.file.common.errors.DelayException;
import com.bin.delay.file.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CheckpointFile {
    private File file;
    private String logDir;
    private Object  lock = new Object();
    private  Path tempPath;
    private Path path;
    public CheckpointFile(File file,String logDir) {
       path = file.toPath().toAbsolutePath();
       tempPath = Paths.get(path.toString() + ".tmp");

    }


    public synchronized void write(List<Object> entries) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8));
            try {
                writer.write(entries.size());
                writer.newLine();


                writer.flush();
                fileOutputStream.getFD().sync();
            }finally {
                writer.close();
            }
            Utils.atomicMoveWithFallback(tempPath, path);
        }catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + file.getAbsolutePath();

            throw new DelayException(msg,e);
        }
    }

    public synchronized  List<Object> read() {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            String line = null;
            List<Object> entries = new ArrayList<>();
            try {
                line = reader.readLine();
                if (line == null)
                    return Collections.emptyList();


                return entries;
            }finally {
                reader.close();
            }
        }catch (IOException e) {
            String msg = "Error while reading checkpoint file " + file.getAbsolutePath();
            throw new DelayException(msg,e);
        }
    }
}
