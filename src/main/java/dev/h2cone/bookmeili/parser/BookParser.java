package dev.h2cone.bookmeili.parser;

import com.vip.vjtools.vjkit.text.EncodeUtil;
import com.vip.vjtools.vjkit.text.HashUtil;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * @author huanghe1
 */
public interface BookParser {
    String parsedKey = "parsed";
    String coverKey = "cover";
    String coverFileExt = ".jpg";

    Map<String, Object> parse(Path path, String tempPrefix);

    static Map<String, Object> newBook(Path path) {
        Map<String, Object> book = new HashMap<>(5);
        try (InputStream in = Files.newInputStream(path)) {
            String id = EncodeUtil.encodeHex(HashUtil.sha1File(in));
            book.put("id", id);
            book.put("filePath", path.toString());
            Path filename = path.getFileName();
            book.put("fileName", filename.toString());
            book.put("fileSize", Files.size(path));
            book.put("modifiedTime", Files.getLastModifiedTime(path).toMillis());
        } catch (IOException e) {
            Log.error("failed to read file: " + path, e);
        }
        return book;
    }
}
