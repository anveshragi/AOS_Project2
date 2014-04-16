import java.io.Serializable;

public class VectorClock implements Serializable {

	private static final long serialVersionUID = 8124713416945098120L;
	private int node;
	private int counter;
	
	public VectorClock(int node, int counter) {
		super();
		this.node = node;
		this.counter = counter;
	}

	public VectorClock() {
		super();
		// TODO Auto-generated constructor stub
	}

	public int getNode() {
		return node;
	}

	public void setNode(int node) {
		this.node = node;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}
	
}
