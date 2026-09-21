import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Tema1 {
    private static final Queue<String> fileTaskQueue = new ConcurrentLinkedQueue<>();

    private static final ConcurrentHashMap<String, Integer> globalUuidRegister = new ConcurrentHashMap<>(50000);
    private static final ConcurrentHashMap<String, Integer> globalTitleRegister = new ConcurrentHashMap<>(50000);

    private static Set<String> targetLangs;
    private static Set<String> targetCats;
    private static Set<String> stopWords;

    private static final List<Article> masterList = new ArrayList<>();

    private static final Map<String, List<String>> finalLangMap = new HashMap<>();
    private static final Map<String, List<String>> finalCatMap = new HashMap<>();
    private static final Map<String, Integer> finalKeywords = new HashMap<>();
    private static final Map<String, Integer> finalAuthors = new HashMap<>();
    private static final Map<String, Integer> finalLangStats = new HashMap<>();
    private static final Map<String, Integer> finalCatStats = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java Tema1 <P> <art> <in>");
            return;
        }

        int p = Integer.parseInt(args[0]);
        initializeConfig(args[1], args[2]);

        CyclicBarrier barrier = new CyclicBarrier(p);
        Thread[] workers = new Thread[p];
        List<MapReduceWorker> tasks = new ArrayList<>(p);

        for (int i = 0; i < p; i++) {
            MapReduceWorker task = new MapReduceWorker(barrier);
            tasks.add(task);
            workers[i] = new Thread(task);
            workers[i].start();
        }

        for (Thread t : workers) t.join();

        int rawTotal = 0;

        for (MapReduceWorker w : tasks) {
            rawTotal += w.itemsRead;
            masterList.addAll(w.validItems);

            w.myLang.forEach((k, v) -> finalLangMap.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
            w.myCat.forEach((k, v) -> finalCatMap.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
            w.myKeys.forEach((k, v) -> finalKeywords.merge(k, v, Integer::sum));
            w.myAuth.forEach((k, v) -> finalAuthors.merge(k, v, Integer::sum));
            w.myLangStats.forEach((k, v) -> finalLangStats.merge(k, v, Integer::sum));
            w.myCatStats.forEach((k, v) -> finalCatStats.merge(k, v, Integer::sum));
        }

        writeFiles(rawTotal - masterList.size());
    }

    static class MapReduceWorker implements Runnable {
        private final CyclicBarrier barrier;
        private final ObjectMapper jackson;

        List<Article> rawBuffer = new ArrayList<>();
        List<Article> validItems = new ArrayList<>();
        int itemsRead = 0;

        Map<String, List<String>> myLang = new HashMap<>();
        Map<String, List<String>> myCat = new HashMap<>();
        Map<String, Integer> myKeys = new HashMap<>();
        Map<String, Integer> myAuth = new HashMap<>();
        Map<String, Integer> myLangStats = new HashMap<>();
        Map<String, Integer> myCatStats = new HashMap<>();

        public MapReduceWorker(CyclicBarrier barrier) {
            this.barrier = barrier;
            this.jackson = new ObjectMapper();
        }

        @Override
        public void run() {
            try {
                String path;
                while ((path = fileTaskQueue.poll()) != null) {
                    File f = new File(path);
                    if (!f.exists()) continue;

                    List<Article> batch = jackson.readValue(f, new TypeReference<List<Article>>() {});

                    for (Article a : batch) {
                        rawBuffer.add(a);
                        itemsRead++;

                        if (a.getUuid() != null)
                            globalUuidRegister.merge(a.getUuid(), 1, Integer::sum);
                        if (a.getTitle() != null)
                            globalTitleRegister.merge(a.getTitle(), 1, Integer::sum);
                    }
                }

                barrier.await();

                for (Article a : rawBuffer) {
                    int c1 = (a.getUuid() == null) ? 0 : globalUuidRegister.get(a.getUuid());
                    int c2 = (a.getTitle() == null) ? 0 : globalTitleRegister.get(a.getTitle());

                    if (c1 <= 1 && c2 <= 1) {
                        validItems.add(a);
                        extractData(a);
                    }
                }

                rawBuffer = null;
                barrier.await();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void extractData(Article a) {
            if (a.getAuthor() != null) myAuth.merge(a.getAuthor(), 1, Integer::sum);

            String l = a.getLanguage();
            if (l != null && targetLangs.contains(l)) {
                myLang.computeIfAbsent(l, k -> new ArrayList<>()).add(a.getUuid());
                myLangStats.merge(l, 1, Integer::sum);

                if ("english".equals(l)) parseText(a.getText());
            }

            if (a.getCategories() != null) {
                Set<String> visited = new HashSet<>();
                for (String c : a.getCategories()) {
                    String norm = c.replace(",", "").trim().replace(" ", "_");
                    if (targetCats.contains(c.trim()) && visited.add(norm)) {
                        myCat.computeIfAbsent(norm, k -> new ArrayList<>()).add(a.getUuid());
                        myCatStats.merge(norm, 1, Integer::sum);
                    }
                }
            }
        }

        private void parseText(String text) {
            if (text == null || text.isEmpty()) return;

            String[] tokens = text.toLowerCase(Locale.ROOT).split("\\s+");
            Set<String> found = new HashSet<>();

            for (String t : tokens) {
                String clean = t.replaceAll("[^a-z]", "");
                if (!clean.isEmpty() && !stopWords.contains(clean)) {
                    found.add(clean);
                }
            }

            for (String w : found) myKeys.merge(w, 1, Integer::sum);
        }
    }

    private static void initializeConfig(String fArt, String fIn) throws IOException {
        Path base = Paths.get(fIn).getParent();
        List<String> cfg = readSimpleLines(fIn);

        targetLangs = loadSet(resolve(base, cfg.get(0)), false);
        targetCats = loadSet(resolve(base, cfg.get(1)), false);
        stopWords = loadSet(resolve(base, cfg.get(2)), true);

        Path artBase = Paths.get(fArt).getParent();
        try (BufferedReader br = new BufferedReader(new FileReader(fArt))) {
            br.readLine();
            String l;
            while ((l = br.readLine()) != null) {
                if (!l.trim().isEmpty())
                    fileTaskQueue.add(resolve(artBase, l.trim()));
            }
        }
    }

    private static String resolve(Path d, String f) {
        return (d == null) ? f : d.resolve(f).normalize().toString();
    }

    private static Set<String> loadSet(String p, boolean low) throws IOException {
        Set<String> s = new HashSet<>();
        for (String l : readSimpleLines(p)) {
            if (l != null) s.add(low ? l.toLowerCase(Locale.ROOT) : l.trim());
        }
        return s;
    }

    private static List<String> readSimpleLines(String p) throws IOException {
        List<String> l = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(p))) {
            br.readLine();
            String s;
            while ((s = br.readLine()) != null) l.add(s.trim());
        }
        return l;
    }


    private static void writeFiles(int dup) throws IOException {
        masterList.sort((a, b) -> {
            String d1 = a.getPublished();
            String d2 = b.getPublished();
            if (d1 == null) return 1; if (d2 == null) return -1;
            int c = d2.compareTo(d1);
            return (c != 0) ? c : a.getUuid().compareTo(b.getUuid());
        });

        int buf = 65536;

        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("all_articles.txt"), buf))) {
            for (Article a : masterList) pw.println(a.getUuid() + " " + a.getPublished());
        }

        dumpMap(finalLangMap, ".txt");
        dumpMap(finalCatMap, ".txt");

        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("keywords_count.txt"), buf))) {
            finalKeywords.entrySet().stream()
                    .sorted((a, b) -> {
                        int c = b.getValue().compareTo(a.getValue());
                        return c != 0 ? c : a.getKey().compareTo(b.getKey());
                    })
                    .forEach(e -> pw.println(e.getKey() + " " + e.getValue()));
        }

        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("reports.txt"), buf))) {
            pw.println("duplicates_found - " + dup);
            pw.println("unique_articles - " + masterList.size());
            pw.println("best_author - " + getTop(finalAuthors));
            pw.println("top_language - " + getTop(finalLangStats));
            pw.println("top_category - " + getTop(finalCatStats));

            Article r = masterList.isEmpty() ? null : masterList.get(0);
            pw.println("most_recent_article - " + (r == null ? "- -" : r.getPublished() + " " + r.getUrl()));
            pw.println("top_keyword_en - " + getTop(finalKeywords));
        }
    }

    private static void dumpMap(Map<String, List<String>> m, String ext) throws IOException {
        for (var e : m.entrySet()) {
            Collections.sort(e.getValue());
            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(e.getKey() + ext), 32768))) {
                for (String s : e.getValue()) pw.println(s);
            }
        }
    }

    private static String getTop(Map<String, Integer> m) {
        if (m.isEmpty()) return "";
        String k = null; int v = -1;
        for (var e : m.entrySet()) {
            if (e.getValue() > v) {
                v = e.getValue(); k = e.getKey();
            } else if (e.getValue() == v) {
                if (k == null || e.getKey().compareTo(k) < 0) k = e.getKey();
            }
        }
        return k + " " + v;
    }
}
