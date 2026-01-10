import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;

class RecordUser {
    public Optional<JsonNode> name;
    public Long age;
}

public class Record {
    public String id;
    public RecordUser user;
    public Optional<Double> price;
    public Boolean active;
    public Optional<JsonNode> meta;
    @JsonProperty("user-name")
    public Optional<JsonNode> userName;
    @JsonProperty("class")
    public Optional<JsonNode> class_;
    public String status;
    public String source;
}
