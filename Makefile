CP = .:lib/*

build:
	javac -cp "$(CP)" *.java

run:
	java -cp "$(CP)" Tema1 $(ARGS)

clean:
	rm -f *.class *.txt
