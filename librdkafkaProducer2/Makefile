INC_DIR:= ./
SRC_DIR:= ./
SRCS:=$(wildcard *.cpp)
OBJS:=$(patsubst %.cpp, %.o, $(SRCS))
LIBS:=-lrdkafka -lz -lpthread -lrt

CXX:=g++

CXXFLAGS:= -w -g -std=c++11 $(addprefix -I, $(INC_DIR)) -L ./ $(LIBS) -Wno-deprecated

EXE:=./producer

$(EXE):$(OBJS)
	$(CXX) -o $(EXE) $(OBJS) $(CXXFLAGS)

clean:
	rm -rf $(EXE)
	rm -rf $(OBJS)
