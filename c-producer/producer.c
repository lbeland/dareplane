/*
 * High-performance LSL producer for benchmarking
 * Based on the dareplane Python producer but implemented in C for higher throughput
 */

#include "../dp-c-sdl2-example/include/lsl_c.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

// Configuration
static int n_messages = 1000;
static double freq = 1000.0;
static int n_channels = 1;
static int msg_size = 1;
static char output_file[256] = "send_times.csv";

// Timing storage
static double *send_times = NULL;
static int send_count = 0;

// Signal handling for clean shutdown
static volatile int keep_running = 1;

void handle_signal(int sig) {
    keep_running = 0;
    printf("\nCaught signal %d, shutting down...\n", sig);
}

// Get high-precision monotonic time in seconds
double get_monotonic_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// Precise sleep until target time
void sleep_until(double target_time) {
    double now;
    struct timespec ts;
    
    // Busy-wait for last microseconds for better precision
    while ((now = get_monotonic_time()) < target_time) {
        if (target_time - now > 0.001) {  // More than 1ms away
            ts.tv_sec = 0;
            ts.tv_nsec = (long)((target_time - now - 0.0005) * 1e9);
            nanosleep(&ts, NULL);
        }
    }
}

void save_results() {
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        fprintf(stderr, "Error opening output file: %s\n", output_file);
        return;
    }
    
    fprintf(fp, "send_time,send_period\n");
    for (int i = 0; i < send_count; i++) {
        double period = (i < send_count - 1) ? (send_times[i+1] - send_times[i]) : 0.0;
        fprintf(fp, "%.9f,%.9f\n", send_times[i], period);
    }
    fclose(fp);
    
    // Print statistics (excluding first sample)
    if (send_count > 1) {
        double sum = 0.0, sum_sq = 0.0, max_period = 0.0;
        int max_idx = 0;
        
        for (int i = 1; i < send_count - 1; i++) {
            double period = send_times[i+1] - send_times[i];
            sum += period;
            sum_sq += period * period;
            if (period > max_period) {
                max_period = period;
                max_idx = i;
            }
        }
        
        int n = send_count - 2;
        double mean = sum / n;
        double variance = (sum_sq / n) - (mean * mean);
        double std = sqrt(variance);
        
        printf("\n=== Producer Statistics ===\n");
        printf("Total messages sent: %d\n", send_count);
        printf("Average send period (s): %.6f\n", mean);
        printf("Max send period (s): %.6f, idx: %d\n", max_period, max_idx);
        printf("Std send period (s): %.6f\n", std);
    }
}

void parse_args(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--n_messages=", 13) == 0) {
            n_messages = atoi(argv[i] + 13);
        } else if (strncmp(argv[i], "--freq=", 7) == 0) {
            freq = atof(argv[i] + 7);
        } else if (strncmp(argv[i], "--n_channels=", 13) == 0) {
            n_channels = atoi(argv[i] + 13);
        } else if (strncmp(argv[i], "--msg_size=", 11) == 0) {
            msg_size = atoi(argv[i] + 11);
        } else if (strncmp(argv[i], "--output_file=", 14) == 0) {
            strncpy(output_file, argv[i] + 14, sizeof(output_file) - 1);
        }
    }
    
    printf("Configuration:\n");
    printf("  n_messages: %d\n", n_messages);
    printf("  freq: %.1f Hz\n", freq);
    printf("  n_channels: %d\n", n_channels);
    printf("  msg_size: %d\n", msg_size);
    printf("  output_file: %s\n", output_file);
}

int main(int argc, char *argv[]) {
    // Parse command line arguments
    parse_args(argc, argv);
    
    // Set up signal handlers
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    // Allocate timing storage
    send_times = (double *)malloc(n_messages * sizeof(double));
    if (!send_times) {
        fprintf(stderr, "Failed to allocate memory for send_times\n");
        return EXIT_FAILURE;
    }
    
    // Create LSL outlet
    printf("Creating LSL outlet...\n");
    lsl_streaminfo info = lsl_create_streaminfo(
        "mock_random1",           // stream name (matches consumer config)
        "EEG",                    // stream type
        n_channels,               // number of channels
        freq,                     // nominal sample rate
        cft_double64,             // channel format
        "c_producer_uid"          // unique source ID
    );
    
    lsl_outlet outlet = lsl_create_outlet(info, 0, 360);
    printf("LSL outlet created. Waiting for consumer...\n");
    
    // Wait for at least one consumer to connect
    if (!lsl_wait_for_consumers(outlet, 5.0)) {
        fprintf(stderr, "No consumers connected within 5 seconds\n");
        lsl_destroy_outlet(outlet);
        free(send_times);
        return EXIT_FAILURE;
    }
    
    printf("Consumer connected. Starting data transmission...\n");
    
    // Allocate data buffer
    double *data = (double *)malloc(n_channels * sizeof(double));
    if (!data) {
        fprintf(stderr, "Failed to allocate memory for data\n");
        lsl_destroy_outlet(outlet);
        free(send_times);
        return EXIT_FAILURE;
    }
    
    // Main transmission loop
    double period_s = 1.0 / freq;
    double next_deadline = get_monotonic_time();
    
    for (int i = 0; i < n_messages && keep_running; i++) {
        // Wait for next deadline
        sleep_until(next_deadline);
        
        // Get timestamp and fill data with it
        double now = get_monotonic_time();
        for (int ch = 0; ch < n_channels; ch++) {
            data[ch] = now;
        }
        
        // Send data via LSL
        lsl_push_sample_d(outlet, data);
        
        // Record send time
        send_times[send_count++] = now;
        
        // Update deadline for next sample
        next_deadline += period_s;
        
        // Progress indicator
        if ((i + 1) % 100 == 0) {
            printf("Sent %d/%d messages\r", i + 1, n_messages);
            fflush(stdout);
        }
    }
    
    printf("\nTransmission complete. Waiting for consumers to disconnect...\n");
    
    // Give consumer time to receive last samples
    sleep(1);
    
    // Cleanup
    free(data);
    lsl_destroy_outlet(outlet);
    
    // Save results
    save_results();
    free(send_times);
    
    printf("Producer finished.\n");
    return EXIT_SUCCESS;
}
