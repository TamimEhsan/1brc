#include<bits/stdc++.h>
using namespace std;

#define BUF_SIZE 1024

struct City {
    float sum;
    int count;
    float min;
    float max;
};

map<string, City> city_map;

void updateCities(string city, float temperature) {
    if(city_map.find(city) == city_map.end()) {
        city_map[city] = {temperature, 1, temperature, temperature};
        return;
    }
    City &city_data = city_map[city];
    city_data.sum += temperature;
    city_data.count++;
    city_data.min = min(city_data.min, temperature);
    city_data.max = max(city_data.max, temperature);
}

void processLine(char *line) {
    char *city = strtok(line, ";");
    char *temp_str = strtok(NULL, ";");
    
    if (city && temp_str) {
        // Parse temperature
        float temperature = strtof(temp_str, NULL);
        //printf("City: %s, Temperature: %.1f\n", city, temperature);
        updateCities(city, temperature);
    }
}

void challenge(string fileName) {
    FILE *file = fopen(fileName.c_str(), "r");
    if (!file) {
        perror("Failed to open file");
        exit(1);
    }
    int line_no = 0;
    int block_no = 0;
    // Buffer to read the file
    char buffer[BUF_SIZE*2];
    size_t bytesRead;

    size_t leftover_len = 0;   // Tracks the length of leftover data

    while ((bytesRead = fread(buffer+leftover_len, 1, BUF_SIZE, file)+leftover_len) > 0) {
        size_t idx = 0;
        leftover_len = 0;
        // Handle any leftover data from the previous read

        while (idx < bytesRead) {
            // Read until a newline or end of buffer
            char *line_start = &buffer[idx];
            char *line_end = strchr(line_start, '\n');

            if (line_end == NULL) {
                break; // If no newline is found, break out of the loop and process next chunk
            }
            
            *line_end = '\0'; // Terminate the current line with null-terminator

            int start = idx;
            int end = line_end - buffer;

            // copy the line to a new buffer
            char line[100];
            memcpy(line, &buffer[start], end - start + 1);
        
            processLine(line);

            // Move to the next line in the buffer
            idx = end + 1;

            line_no++;
        }

        // If we reached the end of the buffer and there's incomplete data, store it for the next iteration
        if (idx < bytesRead) {
            size_t remaining_len = bytesRead - idx;
            if (remaining_len > 0) {
                memcpy(buffer, &buffer[idx], remaining_len);
                leftover_len = remaining_len;
            }
        }

        block_no++;
    }

    printf("Total lines processed: %d\n", line_no);
    printf("Total blocks processed: %d\n", block_no);
    fclose(file);
}


void printResults(string fileName) {
    // print to file
    FILE *file = fopen(fileName.c_str(), "w");
    bool first = true;
    fprintf(file, "{");
    for(auto &city : city_map) {
        City &city_data = city.second;
        if(!first) {
            fprintf(file, ", ");
        }else{
            first = false;
        }
        fprintf(file,"%s=%.1f/%.1f/%.1f", city.first.c_str(), city_data.min, city_data.sum/city_data.count, city_data.max);
    }
    fprintf(file, "}");
}

int main() {

    auto start = chrono::high_resolution_clock::now();
    
    challenge("../../1brc/measurements_small.txt");
   
    auto end = chrono::high_resolution_clock::now();
    double time_taken = chrono::duration_cast<chrono::milliseconds>(end - start).count();
    printf("Time taken: %.2f ms\n", time_taken);

    printResults("output.out");

    return 0;
}
