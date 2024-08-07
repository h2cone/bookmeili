package dev.h2cone.bookmeili.parser;

import io.quarkus.logging.Log;
import org.apache.commons.lang3.EnumUtils;

import java.util.*;

/**
 * @author huanghe1
 */
public enum BookFilenameExt {
    /**
     * PDF
     */
    pdf(PdfBookParser.class),
    /**
     * EPUB
     */
    epub(EpubBookParser.class),
    /**
     * MOBI
     */
    mobi(MobiBookParser.class),
    /**
     * AZW3
     */
    azw3(null),
    /**
     * TXT
     */
    txt(null),
    /**
     * DOC
     */
    doc(null),
    /**
     * DOCX
     */
    docx(null),
    /**
     * HTML
     */
    html(null);

    final Class<? extends BookParser> parserClass;

    BookFilenameExt(Class<? extends BookParser> parserClass) {
        this.parserClass = parserClass;
    }

    public static final Map<String, BookParser> EXT_TO_PARSER;

    public static final Set<String> EXTENSIONS;

    static {
        List<BookFilenameExt> extList = EnumUtils.getEnumList(BookFilenameExt.class);
        EXTENSIONS = new HashSet<>(extList.size());
        for (BookFilenameExt ext : extList) {
            String name = ext.name();
            EXTENSIONS.add(name.toLowerCase());
            EXTENSIONS.add(name.toUpperCase());
        }
        EXT_TO_PARSER = new HashMap<>(extList.size());
        for (BookFilenameExt ext : extList) {
            if (Objects.nonNull(ext.parserClass)) {
                try {
                    BookParser parser = ext.parserClass.getDeclaredConstructor().newInstance();
                    EXT_TO_PARSER.put(ext.name(), parser);
                } catch (Exception e) {
                    Log.error("failed to create parser for " + ext.name(), e);
                }
            }
        }
    }
}
