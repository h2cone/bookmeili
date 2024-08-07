package dev.h2cone.bookmeili;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.meilisearch.sdk.Client;
import com.meilisearch.sdk.Config;
import com.meilisearch.sdk.model.TaskInfo;
import com.vip.vjtools.vjkit.collection.CollectionUtil;
import com.vip.vjtools.vjkit.io.FileUtil;
import com.vip.vjtools.vjkit.mapper.JsonMapper;
import dev.h2cone.bookmeili.parser.BookFilenameExt;
import dev.h2cone.bookmeili.parser.BookParser;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import io.quarkus.logging.Log;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * @author huanghe1
 */
@CommandLine.Command(name = Application.NAME, description = "Add books to meilisearch index.", mixinStandardHelpOptions = true, version = "1.0")
public class Application implements Callable<Integer> {
    @CommandLine.Option(names = "-c", defaultValue = "./app.yml", description = "Path to config file.")
    String cfgPath;
    @CommandLine.Parameters(paramLabel = "<book>", description = "Path to book file or directory.")
    String[] books;

    static final String NAME = "bookmeili";
    static final ObjectMapper YAML_MAPPER;

    static {
        YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    }

    JsonNode cfg;
    Client searchClient;
    MinioClient minioClient;
    ExecutorService executor;

    public static void main(String[] args) {
        var app = new Application();
        var cli = new CommandLine(app);
        int code = cli.execute(args);
        System.exit(code);
    }

    @Override
    public Integer call() throws Exception {
        init();
        final String indexName = "books", pk = "id";
        TaskInfo taskInfo = searchClient.createIndex(indexName, pk);
        Log.info("creating index task id: " + taskInfo.getTaskUid());

        String documents;
        var idxPath = Paths.get(cfg.path("search-idx-path").asText());
        if (ArrayUtils.isNotEmpty(books)) {
            Log.info("start to parse books, please wait...");
            long now = System.currentTimeMillis();
            Map<String, Map<String, Object>> pathToBook = createPathToBook(books);
            Log.infof("parsed size: %d, cost: %d\n", pathToBook.size(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now));

            Collection<Map<String, Object>> books = pathToBook.values();
            documents = JsonMapper.INSTANCE.getMapper().writeValueAsString(books);
            Files.writeString(idxPath, documents, StandardCharsets.UTF_8);
        } else {
            Log.infof("reading documents from %s\n", idxPath);
            documents = Files.readString(idxPath, StandardCharsets.UTF_8);
        }
        var index = searchClient.index(indexName);
        // warn: index.addDocuments(documents);
        TaskInfo[] infoArr = index.addDocumentsInBatches(documents, 10000, pk);
        Log.info("indexing task ids: " + Arrays.stream(infoArr).map(TaskInfo::getTaskUid).toList());
        return 0;
    }

    Map<String, Map<String, Object>> createPathToBook(String... roots) throws IOException {
        Map<String, Map<String, Object>> pathToBook = new HashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        final String posterKey = "poster", idKey = "id";
        for (String root : roots) {
            try (Stream<Path> pathStream = Files.find(Paths.get(root), Integer.MAX_VALUE, (p, a) ->
                    BookFilenameExt.EXTENSIONS.contains(FileUtil.getFileExtension(p.toString())))) {
                pathStream.forEach(path -> {
                            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                                String filepath = path.toString();
                                String ext = FileUtil.getFileExtension(filepath);
                                BookParser parser = BookFilenameExt.EXT_TO_PARSER.get(ext.toLowerCase());
                                Map<String, Object> book = Objects.nonNull(parser) ? parser.parse(path, NAME) : BookParser.newBook(path);

                                Object coverObj = book.get(BookParser.coverKey);
                                Path cover = Objects.nonNull(coverObj) ? Paths.get(coverObj.toString()) : null;
                                try {
                                    book.put(posterKey, buildPoster(cover, book.get(idKey).toString() + BookParser.coverFileExt));
                                } catch (Exception e) {
                                    Log.error("failed to upload poster: " + cover, e);
                                }
                                pathToBook.put(filepath, book);
                            }, executor);
                            futures.add(future);
                        }
                );
            }
        }
        if (CollectionUtil.isNotEmpty(futures)) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
        return pathToBook;
    }

    void init() throws IOException {
        try (InputStream in = Files.newInputStream(Paths.get(cfgPath))) {
            this.cfg = YAML_MAPPER.readTree(in);

            var url = cfg.path("search-url").asText();
            var apiKey = cfg.path("search-api-key").asText();
            var config = new Config(StringUtils.isNotBlank(url) ? url : "http://localhost:7700", apiKey);
            searchClient = new Client(config);

            minioClient = MinioClient.builder()
                    .endpoint(cfg.path("minio-endpoint").asText())
                    .credentials(cfg.path("minio-access-key").asText(), cfg.path("minio-secret-key").asText())
                    .build();

            JsonNode poolNode = cfg.path("pool");
            int coreSize = poolNode.path("core-size").asInt();
            int maxSize = poolNode.path("max-size").asInt();
            int keepAlive = poolNode.path("keep-alive").asInt();
            int queueCapacity = poolNode.path("queue-capacity").asInt();
            executor = new ThreadPoolExecutor(
                    coreSize,
                    maxSize,
                    keepAlive,
                    TimeUnit.MILLISECONDS,
                    queueCapacity > 0 ? new LinkedBlockingQueue<>(queueCapacity) : new LinkedBlockingQueue<>(),
                    new ThreadFactoryBuilder().setNameFormat(NAME + "-%d").build()
            );
        }
    }

    String buildPoster(Path cover, String object) throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        if (Objects.isNull(cover)) {
            return StringUtils.EMPTY;
        }
        try {
            minioClient.uploadObject(UploadObjectArgs.builder()
                    .bucket(cfg.path("minio-bucket").asText())
                    .object(object)
                    .filename(cover.toString())
                    .build());
        } catch (MinioException e) {
            Log.error("Error occurred: " + e);
            Log.error("HTTP trace: " + e.httpTrace());
        } finally {
            Files.deleteIfExists(cover);
        }
        return cfg.path("minio-endpoint").asText() + "/" + cfg.path("minio-bucket").asText() + "/" + object;
    }
}
