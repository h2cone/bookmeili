package dev.h2cone.bookmeili.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.vip.vjtools.vjkit.io.FileUtil;
import com.vip.vjtools.vjkit.mapper.JsonMapper;
import io.documentnode.epub4j.domain.Book;
import io.documentnode.epub4j.domain.Metadata;
import io.documentnode.epub4j.domain.Resource;
import io.documentnode.epub4j.epub.EpubReader;
import io.quarkus.logging.Log;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;

/**
 * @author huanghe1
 */
public class EpubBookParser implements BookParser {

    @Override
    public Map<String, Object> parse(Path path, String tempPrefix) {
        Map<String, Object> book = BookParser.newBook(path);
        try (InputStream in = Files.newInputStream(path)) {
            Book epub = new EpubReader().readEpub(in);
            Metadata metadata = epub.getMetadata();
            book.putAll(JsonMapper.INSTANCE.getMapper().convertValue(metadata, new TypeReference<>() {
            }));
            Resource coverImage = epub.getCoverImage();
            if (Objects.nonNull(coverImage)) {
                Path target = FileUtil.createTempFile(tempPrefix, BookParser.coverFileExt);
                // when coverImage is null will bocking virtual thread
                Files.copy(coverImage.getInputStream(), target, StandardCopyOption.REPLACE_EXISTING);
                book.put(BookParser.coverKey, target.toString());
                book.put(BookParser.parsedKey, true);
            }
        } catch (Exception e) {
            Log.errorf(e, "failed to parse %s", path);
        }
        return book;
    }
}
