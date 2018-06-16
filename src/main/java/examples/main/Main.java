package examples.main;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {

	public static void main(String[] args) {
		System.out.println("Starting Kafka hello world");
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Config.class);
	}

}
