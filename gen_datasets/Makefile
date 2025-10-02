INCLUDE_DIR     = ./includes
OUT_DIR         = ./bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(INCLUDE_DIR)
OPTFLAGS        = -O3 -finline-functions
LDFLAGS         = -pthread

all: gen

gen.o: gen.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) $< -c

gen: clean gen.o
	$(CXX) gen.o -o $(OUT_DIR)/gen $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/gen
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
