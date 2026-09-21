import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Article {
    @JsonProperty("uuid") private String uid;
    @JsonProperty("title") private String headline;
    @JsonProperty("author") private String writer;
    @JsonProperty("url") private String link;
    @JsonProperty("text") private String content;
    @JsonProperty("published") private String timestamp;
    @JsonProperty("language") private String lang;
    @JsonProperty("categories") private List<String> cats;

    public Article() {}

    public String getUuid() { return uid; }
    public String getTitle() { return headline; }
    public String getAuthor() { return writer; }
    public String getUrl() { return link; }
    public String getText() { return content; }
    public String getPublished() { return timestamp; }
    public String getLanguage() { return lang; }
    public List<String> getCategories() { return cats; }

    @Override public String toString() { return uid; }
}
