package dev.h2cone.bookmeili.parser;

import com.vip.vjtools.vjkit.io.FileUtil;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.ArrayUtils;
import org.rr.mobi4java.MobiDocument;
import org.rr.mobi4java.MobiMetaData;
import org.rr.mobi4java.MobiReader;
import org.rr.mobi4java.exth.ASINRecordDelegate;
import org.rr.mobi4java.exth.ISBNRecordDelegate;
import org.rr.mobi4java.exth.LanguageRecordDelegate;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author huanghe1
 */
public class MobiBookParser implements BookParser {

    @Override
    public Map<String, Object> parse(Path path, String tempPrefix) {
        Map<String, Object> book = BookParser.newBook(path);
        try (InputStream in = Files.newInputStream(path)) {
            MobiDocument mobi = new MobiReader().read(in);
            book.putAll(toMap(mobi));

            byte[] cover = mobi.getCover();
            if (ArrayUtils.isNotEmpty(cover)) {
                Path out = FileUtil.createTempFile(tempPrefix, BookParser.coverFileExt);
                Files.write(out, cover);
                book.put(BookParser.coverKey, out);
                book.put(BookParser.parsedKey, true);
            }
        } catch (Exception e) {
            Log.errorf(e, "failed to parse %s", path);
        }
        return book;
    }

    Map<String, Object> toMap(MobiDocument document) {
        Map<String, Object> more = new HashMap<>();
        more.put("title", document.getFullName());
        MobiMetaData data = document.getMetaData();
        more.put("authors", data.getAuthorRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        data.getASINRecords().stream().map(ASINRecordDelegate::getASIN).findFirst().ifPresent(asin -> more.put("asin", asin));
        more.put("contributors", data.getContributorRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        more.put("descriptions", data.getDescriptionRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        data.getISBNRecords().stream().map(ISBNRecordDelegate::getIsbn).findFirst().stream().findFirst().ifPresent(isbn -> more.put("isbn", isbn));
        data.getImprintRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).findFirst().ifPresent(imprint -> more.put("imprint", imprint));
        LanguageRecordDelegate languageRecord = data.getLanguageRecord();
        if (Objects.nonNull(languageRecord)) {
            more.put("language", languageRecord.getLanguageCode());
        }
        data.getReviewRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).findFirst().ifPresent(review -> more.put("review", review));
        more.put("rights", data.getRightsRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        data.getSourceRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).findFirst().ifPresent(source -> more.put("source", source));
        more.put("subjects", data.getSubjectRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        more.put("publishers", data.getPublisherRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).collect(Collectors.toList()));
        data.getPublishingDateRecords().stream().map(r -> r.getAsString(StandardCharsets.UTF_8.name())).findFirst().ifPresent(publishingDate -> more.put("publishingDate", publishingDate));
        return more;
    }
}
