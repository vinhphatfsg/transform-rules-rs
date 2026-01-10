import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode

data class RecordUser(
    val name: JsonNode?,
    val age: Long
)

data class Record(
    val id: String,
    val user: RecordUser,
    val price: Double?,
    val active: Boolean,
    val meta: JsonNode?,
    @JsonProperty("user-name")
    val userName: JsonNode?,
    @JsonProperty("class")
    val class_: JsonNode?,
    val status: String,
    val source: String
)
