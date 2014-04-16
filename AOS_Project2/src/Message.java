import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = -1282763823648610278L;
	private String msg_identifier;
	private String key;
	private String value;
	private VectorClock vectorClock;
	
	public Message(String msg_identifier, String key, String value, VectorClock vectorClock) {
		super();
		this.msg_identifier = msg_identifier;
		this.key = key;
		this.value = value;
		this.vectorClock = vectorClock;
	}
	
	public Message() {
		super();
		// TODO Auto-generated constructor stub
	}

	
	public String getMsg_identifier() {
		return msg_identifier;
	}

	public void setMsg_identifier(String msg_identifier) {
		this.msg_identifier = msg_identifier;
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}

	public VectorClock getVectorClock() {
		return vectorClock;
	}

	public void setVectorClock(VectorClock vectorClock) {
		this.vectorClock = vectorClock;
	}
}
