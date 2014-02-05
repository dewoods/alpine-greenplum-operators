PROG=GreenplumDataOperators.jar
PACKAGE=com/alpine/plugins
JC = javac
PWD = $(shell pwd)
.SUFFIXES: .class

PLUGINS = GreenplumInsertPlugin.java GreenplumUpdatePlugin.java GreenplumMergePlugin.java
	    
all: jar/$(PROG)

jar/$(PROG): $(PLUGINS:%.java=%.class)
	cd $(PWD)/build; \
	jar cvfe $(PWD)/jar/$(PROG) $(PROG:.jar=) $(PLUGINS:%.java=$(PACKAGE)/%.class) 

$(PLUGINS:%.java=%.class):
	cd $(PWD)/src/main/java; \
	$(JC) $(PACKAGE)/$*.java -d $(PWD)/build/

clean:
	rm -rf jar/*
	rm -rf build/*
