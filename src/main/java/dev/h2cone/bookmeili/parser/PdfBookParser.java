package dev.h2cone.bookmeili.parser;

import com.lowagie.text.pdf.PdfReader;
import com.vip.vjtools.vjkit.io.FileUtil;
import io.quarkus.logging.Log;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.file.Path;
import java.util.Map;

/**
 * @author huanghe1
 */
public class PdfBookParser implements BookParser {

    @Override
    public Map<String, Object> parse(Path path, String tempPrefix) {
        Map<String, Object> book = BookParser.newBook(path);
        String filepath = path.toString();
        try (PdfReader reader = new PdfReader(filepath); PDDocument pd = Loader.loadPDF((new File(filepath)))) {
            book.putAll(reader.getInfo());

            PDFRenderer pr = new PDFRenderer(pd);
            BufferedImage bi = pr.renderImageWithDPI(0, 300);
            Path out = FileUtil.createTempFile(tempPrefix, BookParser.coverFileExt);
            String outStr = out.toString();
            ImageIO.write(bi, "jpg", new File(outStr));
            book.put(BookParser.coverKey, outStr);
            book.put(BookParser.parsedKey, true);
        } catch (Exception e) {
            Log.errorf(e, "failed to parse %s", filepath);
        }
        return book;
    }
}
