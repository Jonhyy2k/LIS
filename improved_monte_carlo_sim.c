#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <ctype.h>
#include <getopt.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#define MAX_LINE_LENGTH 1000
#define MAX_TICKER_LENGTH 20
#define MAX_YEARS 30
#define DEFAULT_SIMULATIONS 10000
#define DEFAULT_OUTPUT_FILE "Monte_Carlo_Results.txt"
#define DEFAULT_GRAPH_WIDTH 60
#define DEFAULT_GRAPH_HEIGHT 20
#define DEFAULT_VOLATILITY_FACTOR 1.5

typedef struct {
    char ticker[MAX_TICKER_LENGTH];
    int num_years;
    double growth_rates[MAX_YEARS];
    int years[MAX_YEARS];
} StockData;

typedef struct {
    double mean;
    double std_dev;
    double min;
    double max;
    double percentile_5;
    double percentile_25;
    double percentile_50;
    double percentile_75;
    double percentile_95;
    double var_95;
    double var_99;
} Statistics;

typedef struct {
    int num_simulations;
    double volatility_factor;
    int graph_width;
    int graph_height;
    char input_file[MAX_LINE_LENGTH];
    char output_file[MAX_LINE_LENGTH];
    int export_csv;
    int verbose;
    int num_threads;
} SimulationConfig;

void print_usage(const char* program_name) {
    printf("Usage: %s [OPTIONS]\n", program_name);
    printf("Monte Carlo stock metrics simulation tool\n\n");
    printf("Options:\n");
    printf("  -i, --input FILE        Input file with stock forecasts (default: Forecasts.txt)\n");
    printf("  -o, --output FILE       Output file for results (default: Monte_Carlo_Results.txt)\n");
    printf("  -s, --simulations NUM   Number of simulations to run (default: 10000)\n");
    printf("  -v, --volatility FACTOR Volatility factor (default: 1.5)\n");
    printf("  -w, --width NUM         Histogram width (default: 60)\n");
    printf("  -h, --height NUM        Histogram height (default: 20)\n");
    printf("  -c, --csv               Export results to CSV for external plotting\n");
    printf("  -t, --threads NUM       Number of threads to use (default: available cores)\n");
    printf("  -V, --verbose           Display detailed progress information\n");
    printf("  -?, --help              Display this help message\n");
}

int compare_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

double generate_normal(double mean, double std_dev) {
    static int has_spare = 0;
    static double spare;
    
    if (has_spare) {
        has_spare = 0;
        return spare * std_dev + mean;
    }
    
    has_spare = 1;
    double u, v, mag;
    do {
        u = 2.0 * ((double)rand() / RAND_MAX) - 1.0;
        v = 2.0 * ((double)rand() / RAND_MAX) - 1.0;
        mag = u * u + v * v;
    } while (mag >= 1.0 || mag == 0.0);
    
    mag = sqrt(-2.0 * log(mag) / mag);
    spare = v * mag;
    return mean + std_dev * u * mag;
}

Statistics calculate_statistics(double *values, int n) {
    Statistics stats = {0};
    
    if (n <= 0) {
        fprintf(stderr, "Error: Cannot calculate statistics on empty dataset\n");
        return stats;
    }
    
    // Sort values for percentile calculations
    qsort(values, n, sizeof(double), compare_doubles);
    
    // Calculate mean
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
        sum += values[i];
    }
    stats.mean = sum / n;
    
    // Calculate standard deviation
    double variance = 0.0;
    for (int i = 0; i < n; i++) {
        double diff = values[i] - stats.mean;
        variance += diff * diff;  // Avoid pow() for better performance
    }
    stats.std_dev = sqrt(variance / (n - 1));
    
    // Min and Max
    stats.min = values[0];
    stats.max = values[n - 1];
    
    // Percentiles
    stats.percentile_5 = values[(int)(0.05 * n)];
    stats.percentile_25 = values[(int)(0.25 * n)];
    stats.percentile_50 = values[(int)(0.50 * n)];
    stats.percentile_75 = values[(int)(0.75 * n)];
    stats.percentile_95 = values[(int)(0.95 * n)];
    
    // Value at Risk (VaR) - loss percentiles
    stats.var_95 = -values[(int)(0.05 * n)];
    stats.var_99 = -values[(int)(0.01 * n)];
    
    return stats;
}

void create_histogram(double *values, int n, FILE *output, int width, int height) {
    if (n <= 0) {
        fprintf(stderr, "Error: Cannot create histogram from empty dataset\n");
        return;
    }
    
    double min_val = values[0];
    double max_val = values[n - 1];
    double range = max_val - min_val;
    
    if (range <= 0) {
        fprintf(stderr, "Warning: Zero range in histogram data, using default range\n");
        range = 1.0;  // Default to prevent division by zero
    }
    
    int *bins = calloc(width, sizeof(int));
    if (!bins) {
        fprintf(stderr, "Error: Memory allocation failed for histogram bins\n");
        return;
    }
    
    // Fill bins
    for (int i = 0; i < n; i++) {
        int bin = (int)((values[i] - min_val) / range * (width - 1));
        if (bin >= 0 && bin < width) {
            bins[bin]++;
        }
    }
    
    // Find max frequency for scaling
    int max_freq = 0;
    for (int i = 0; i < width; i++) {
        if (bins[i] > max_freq) {
            max_freq = bins[i];
        }
    }
    
    fprintf(output, "\nDISTRIBUTION HISTOGRAM:\n");
    fprintf(output, "========================\n");
    
    // Print histogram
    for (int row = height - 1; row >= 0; row--) {
        fprintf(output, "%3d%% |", (row * 100) / height);
        for (int col = 0; col < width; col++) {
            int bar_height = max_freq > 0 ? (bins[col] * height) / max_freq : 0;
            if (bar_height > row) {
                fprintf(output, "*");
            } else {
                fprintf(output, " ");
            }
        }
        fprintf(output, "|\n");
    }
    
    // Print x-axis
    fprintf(output, "     +");
    for (int i = 0; i < width; i++) {
        fprintf(output, "-");
    }
    fprintf(output, "+\n");
    fprintf(output, "    %.1f%%", min_val);
    for (int i = 0; i < width - 10; i++) {
        fprintf(output, " ");
    }
    fprintf(output, "%.1f%%\n\n", max_val);
    
    free(bins);
}

void export_csv(const char *ticker, double *values, int n, const SimulationConfig *config) {
    char csv_filename[MAX_LINE_LENGTH + 50];
    snprintf(csv_filename, sizeof(csv_filename), "%s_%s.csv", ticker, "simulation_results");
    
    FILE *csv_file = fopen(csv_filename, "w");
    if (!csv_file) {
        fprintf(stderr, "Error: Could not create CSV file %s\n", csv_filename);
        return;
    }
    
    fprintf(csv_file, "Simulation,FinalValue\n");
    for (int i = 0; i < n; i++) {
        fprintf(csv_file, "%d,%.4f\n", i + 1, values[i]);
    }
    
    fclose(csv_file);
    printf("CSV data exported to %s\n", csv_filename);
}

int parse_stock_data(const char *filename, StockData **stocks_ptr, int max_stocks) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Error: Could not open file %s\n", filename);
        return 0;
    }
    
    StockData *stocks = calloc(max_stocks, sizeof(StockData));
    if (!stocks) {
        fprintf(stderr, "Error: Memory allocation failed for stock data\n");
        fclose(file);
        return 0;
    }
    
    char line[MAX_LINE_LENGTH];
    int stock_count = 0;
    int in_forecast = 0;
    
    while (fgets(line, sizeof(line), file) && stock_count < max_stocks) {
        // Remove newline character
        line[strcspn(line, "\n")] = 0;
        
        // Check for new forecast section
        if (strstr(line, "REVENUE FORECAST FOR")) {
            in_forecast = 1;
            // Extract ticker name
            char *ticker_start = strstr(line, "FOR ") + 4;
            char *ticker_end = strstr(ticker_start, " (");
            if (ticker_start && ticker_end) {
                int ticker_len = ticker_end - ticker_start;
                if (ticker_len < MAX_TICKER_LENGTH - 1) {
                    strncpy(stocks[stock_count].ticker, ticker_start, ticker_len);
                    stocks[stock_count].ticker[ticker_len] = '\0';
                    stocks[stock_count].num_years = 0;
                } else {
                    fprintf(stderr, "Warning: Ticker name too long, truncating: %.*s\n", ticker_len, ticker_start);
                    strncpy(stocks[stock_count].ticker, ticker_start, MAX_TICKER_LENGTH - 1);
                    stocks[stock_count].ticker[MAX_TICKER_LENGTH - 1] = '\0';
                    stocks[stock_count].num_years = 0;
                }
            }
            continue;
        }
        
        // Check for end of section
        if (strstr(line, "---") && in_forecast) {
            if (stocks[stock_count].num_years > 0) {
                stock_count++;
            }
            in_forecast = 0;
            continue;
        }
        
        // Parse year and growth rate
        if (in_forecast && strlen(line) > 0) {
            int year;
            double growth;
            if ((sscanf(line, "%d: %lf%%", &year, &growth) == 2 || 
                 sscanf(line, "%d %lf%%", &year, &growth) == 2) && 
                stocks[stock_count].num_years < MAX_YEARS) {
                int idx = stocks[stock_count].num_years;
                stocks[stock_count].years[idx] = year;
                stocks[stock_count].growth_rates[idx] = growth;
                stocks[stock_count].num_years++;
            }
        }
    }
    
    // Check if we ended on an active forecast section
    if (in_forecast && stocks[stock_count].num_years > 0) {
        stock_count++;
    }
    
    fclose(file);
    *stocks_ptr = stocks;
    return stock_count;
}

void run_monte_carlo(StockData *stock, FILE *output, const SimulationConfig *config) {
    if (!stock || !output) {
        fprintf(stderr, "Error: Invalid stock data or output file\n");
        return;
    }
    
    double *final_values = malloc(config->num_simulations * sizeof(double));
    if (!final_values) {
        fprintf(stderr, "Error: Memory allocation failed for simulation results\n");
        return;
    }
    
    double *annual_returns = malloc(config->num_simulations * stock->num_years * sizeof(double));
    if (!annual_returns) {
        fprintf(stderr, "Error: Memory allocation failed for annual returns\n");
        free(final_values);
        return;
    }
    
    fprintf(output, "\n====================================================================================\n");
    fprintf(output, "MONTE CARLO SIMULATION RESULTS FOR %s\n", stock->ticker);
    fprintf(output, "====================================================================================\n");
    fprintf(output, "Number of Simulations: %d\n", config->num_simulations);
    fprintf(output, "Forecast Period: %d-%d (%d years)\n", 
            stock->years[0], stock->years[stock->num_years-1], stock->num_years);
    
    // Calculate base statistics from forecasted growth rates
    double forecast_mean = 0.0;
    for (int i = 0; i < stock->num_years; i++) {
        forecast_mean += stock->growth_rates[i];
    }
    forecast_mean /= stock->num_years;
    
    double forecast_std = 0.0;
    for (int i = 0; i < stock->num_years; i++) {
        double diff = stock->growth_rates[i] - forecast_mean;
        forecast_std += diff * diff;
    }
    forecast_std = sqrt(forecast_std / stock->num_years);
    
    // Add volatility adjustment
    forecast_std *= config->volatility_factor;
    
    fprintf(output, "Base Forecast Mean Growth: %.2f%%\n", forecast_mean);
    fprintf(output, "Adjusted Standard Deviation: %.2f%%\n", forecast_std);
    fprintf(output, "Volatility Factor Applied: %.1fx\n\n", config->volatility_factor);
    
    // Run simulations - use OpenMP if available
    #pragma omp parallel for num_threads(config->num_threads) if(config->num_threads > 1)
    for (int sim = 0; sim < config->num_simulations; sim++) {
        double cumulative_growth = 1.0;
        
        for (int year = 0; year < stock->num_years; year++) {
            // Use forecasted growth as mean with added uncertainty
            double expected_growth = stock->growth_rates[year];
            double simulated_growth = generate_normal(expected_growth, forecast_std);
            
            annual_returns[sim * stock->num_years + year] = simulated_growth;
            cumulative_growth *= (1.0 + simulated_growth / 100.0);
        }
        
        // Final value as percentage change from initial
        final_values[sim] = (cumulative_growth - 1.0) * 100.0;
        
        // Display progress in verbose mode
        if (config->verbose && sim % (config->num_simulations / 10) == 0) {
            #pragma omp critical
            {
                printf("\rRunning simulations for %s: %d%%", stock->ticker, (sim * 100) / config->num_simulations);
                fflush(stdout);
            }
        }
    }
    
    if (config->verbose) {
        printf("\rRunning simulations for %s: 100%%\n", stock->ticker);
    }
    
    // Calculate statistics
    Statistics stats = calculate_statistics(final_values, config->num_simulations);
    
    // Output detailed results
    fprintf(output, "SIMULATION SUMMARY STATISTICS:\n");
    fprintf(output, "------------------------------\n");
    fprintf(output, "Mean Cumulative Growth:     %8.2f%%\n", stats.mean);
    fprintf(output, "Standard Deviation:         %8.2f%%\n", stats.std_dev);
    fprintf(output, "Minimum Growth:             %8.2f%%\n", stats.min);
    fprintf(output, "Maximum Growth:             %8.2f%%\n", stats.max);
    fprintf(output, "\nPERCENTILE ANALYSIS:\n");
    fprintf(output, "--------------------\n");
    fprintf(output, "5th Percentile (Worst 5%%):  %8.2f%%\n", stats.percentile_5);
    fprintf(output, "25th Percentile:            %8.2f%%\n", stats.percentile_25);
    fprintf(output, "50th Percentile (Median):   %8.2f%%\n", stats.percentile_50);
    fprintf(output, "75th Percentile:            %8.2f%%\n", stats.percentile_75);
    fprintf(output, "95th Percentile (Best 5%%):  %8.2f%%\n", stats.percentile_95);
    
    fprintf(output, "\nRISK METRICS:\n");
    fprintf(output, "-------------\n");
    fprintf(output, "Value at Risk (95%% confidence): %8.2f%%\n", stats.var_95);
    fprintf(output, "Value at Risk (99%% confidence): %8.2f%%\n", stats.var_99);
    
    // Probability analysis
    int prob_positive = 0, prob_above_10 = 0, prob_above_20 = 0, prob_below_neg10 = 0;
    for (int i = 0; i < config->num_simulations; i++) {
        if (final_values[i] > 0) prob_positive++;
        if (final_values[i] > 10) prob_above_10++;
        if (final_values[i] > 20) prob_above_20++;
        if (final_values[i] < -10) prob_below_neg10++;
    }
    
    fprintf(output, "\nPROBABILITY ANALYSIS:\n");
    fprintf(output, "---------------------\n");
    fprintf(output, "Probability of Positive Growth:  %6.2f%%\n", (prob_positive * 100.0) / config->num_simulations);
    fprintf(output, "Probability of >10%% Growth:      %6.2f%%\n", (prob_above_10 * 100.0) / config->num_simulations);
    fprintf(output, "Probability of >20%% Growth:      %6.2f%%\n", (prob_above_20 * 100.0) / config->num_simulations);
    fprintf(output, "Probability of <-10%% Loss:       %6.2f%%\n", (prob_below_neg10 * 100.0) / config->num_simulations);
    
    // Create histogram
    create_histogram(final_values, config->num_simulations, output, config->graph_width, config->graph_height);
    
    // Export CSV if requested
    if (config->export_csv) {
        export_csv(stock->ticker, final_values, config->num_simulations, config);
    }
    
    // Year-by-year analysis
    fprintf(output, "YEAR-BY-YEAR ANALYSIS:\n");
    fprintf(output, "======================\n");
    for (int year = 0; year < stock->num_years; year++) {
        double *year_returns = malloc(config->num_simulations * sizeof(double));
        if (!year_returns) {
            fprintf(stderr, "Error: Memory allocation failed for year-by-year analysis\n");
            continue;
        }
        
        for (int sim = 0; sim < config->num_simulations; sim++) {
            year_returns[sim] = annual_returns[sim * stock->num_years + year];
        }
        
        Statistics year_stats = calculate_statistics(year_returns, config->num_simulations);
        
        fprintf(output, "Year %d (Forecast: %.2f%%):\n", stock->years[year], stock->growth_rates[year]);
        fprintf(output, "  Simulated Mean: %7.2f%% | Std Dev: %6.2f%%\n", year_stats.mean, year_stats.std_dev);
        fprintf(output, "  Range: %7.2f%% to %7.2f%% | Median: %7.2f%%\n", 
                year_stats.min, year_stats.max, year_stats.percentile_50);
        
        free(year_returns);
    }
    
    fprintf(output, "\n====================================================================================\n");
    fprintf(output, "END OF ANALYSIS FOR %s\n", stock->ticker);
    fprintf(output, "====================================================================================\n\n\n");
    
    free(final_values);
    free(annual_returns);
}

void parse_args(int argc, char **argv, SimulationConfig *config) {
    static struct option long_options[] = {
        {"input",       required_argument, 0, 'i'},
        {"output",      required_argument, 0, 'o'},
        {"simulations", required_argument, 0, 's'},
        {"volatility",  required_argument, 0, 'v'},
        {"width",       required_argument, 0, 'w'},
        {"height",      required_argument, 0, 'h'},
        {"csv",         no_argument,       0, 'c'},
        {"threads",     required_argument, 0, 't'},
        {"verbose",     no_argument,       0, 'V'},
        {"help",        no_argument,       0, '?'},
        {0, 0, 0, 0}
    };
    
    // Set defaults
    strcpy(config->input_file, "Forecasts.txt");
    strcpy(config->output_file, DEFAULT_OUTPUT_FILE);
    config->num_simulations = DEFAULT_SIMULATIONS;
    config->volatility_factor = DEFAULT_VOLATILITY_FACTOR;
    config->graph_width = DEFAULT_GRAPH_WIDTH;
    config->graph_height = DEFAULT_GRAPH_HEIGHT;
    config->export_csv = 0;
    config->verbose = 0;
    
    // Set number of threads to available cores or 1 if OpenMP not available
    #ifdef _OPENMP
        config->num_threads = omp_get_max_threads();
    #else
        config->num_threads = 1;
    #endif
    
    int opt;
    int option_index = 0;
    
    while ((opt = getopt_long(argc, argv, "i:o:s:v:w:h:ct:V?", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'i':
                strncpy(config->input_file, optarg, MAX_LINE_LENGTH - 1);
                config->input_file[MAX_LINE_LENGTH - 1] = '\0';
                break;
            case 'o':
                strncpy(config->output_file, optarg, MAX_LINE_LENGTH - 1);
                config->output_file[MAX_LINE_LENGTH - 1] = '\0';
                break;
            case 's':
                config->num_simulations = atoi(optarg);
                if (config->num_simulations <= 0) {
                    fprintf(stderr, "Invalid number of simulations. Using default: %d\n", DEFAULT_SIMULATIONS);
                    config->num_simulations = DEFAULT_SIMULATIONS;
                }
                break;
            case 'v':
                config->volatility_factor = atof(optarg);
                if (config->volatility_factor <= 0) {
                    fprintf(stderr, "Invalid volatility factor. Using default: %.1f\n", DEFAULT_VOLATILITY_FACTOR);
                    config->volatility_factor = DEFAULT_VOLATILITY_FACTOR;
                }
                break;
            case 'w':
                config->graph_width = atoi(optarg);
                if (config->graph_width <= 0) {
                    fprintf(stderr, "Invalid graph width. Using default: %d\n", DEFAULT_GRAPH_WIDTH);
                    config->graph_width = DEFAULT_GRAPH_WIDTH;
                }
                break;
            case 'h':
                config->graph_height = atoi(optarg);
                if (config->graph_height <= 0) {
                    fprintf(stderr, "Invalid graph height. Using default: %d\n", DEFAULT_GRAPH_HEIGHT);
                    config->graph_height = DEFAULT_GRAPH_HEIGHT;
                }
                break;
            case 'c':
                config->export_csv = 1;
                break;
            case 't':
                config->num_threads = atoi(optarg);
                if (config->num_threads <= 0) {
                    #ifdef _OPENMP
                        config->num_threads = omp_get_max_threads();
                    #else
                        config->num_threads = 1;
                    #endif
                }
                break;
            case 'V':
                config->verbose = 1;
                break;
            case '?':
                print_usage(argv[0]);
                exit(0);
            default:
                break;
        }
    }
}

int main(int argc, char *argv[]) {
    SimulationConfig config;
    parse_args(argc, argv, &config);
    
    srand(time(NULL));
    
    printf("Monte Carlo Stock Metrics Simulation\n");
    printf("====================================\n");
    
    if (config.verbose) {
        printf("Configuration:\n");
        printf("  Input file: %s\n", config.input_file);
        printf("  Output file: %s\n", config.output_file);
        printf("  Simulations: %d\n", config.num_simulations);
        printf("  Volatility factor: %.2f\n", config.volatility_factor);
        printf("  Graph dimensions: %dx%d\n", config.graph_width, config.graph_height);
        printf("  Export CSV: %s\n", config.export_csv ? "Yes" : "No");
        printf("  Threads: %d\n", config.num_threads);
    }
    
    StockData *stocks = NULL;
    int num_stocks = parse_stock_data(config.input_file, &stocks, 50);
    
    if (num_stocks == 0 || !stocks) {
        fprintf(stderr, "No valid stock data found in %s\n", config.input_file);
        fprintf(stderr, "Make sure the file exists and contains properly formatted forecasts.\n");
        free(stocks);  // Safe to call even if NULL
        return 1;
    }
    
    printf("Found %d stock(s) for analysis:\n", num_stocks);
    for (int i = 0; i < num_stocks; i++) {
        printf("- %s (%d years of forecasts)\n", stocks[i].ticker, stocks[i].num_years);
    }
    
    FILE *output = fopen(config.output_file, "w");
    if (!output) {
        fprintf(stderr, "Error: Could not create output file %s\n", config.output_file);
        free(stocks);
        return 1;
    }
    
    // Write header
    time_t now = time(NULL);
    fprintf(output, "MONTE CARLO SIMULATION ANALYSIS REPORT\n");
    fprintf(output, "Generated: %s", ctime(&now));
    fprintf(output, "Input File: %s\n", config.input_file);
    fprintf(output, "Simulations per Stock: %d\n", config.num_simulations);
    fprintf(output, "Volatility Factor: %.2f\n", config.volatility_factor);
    fprintf(output, "\n");
    
    // Run simulations for each stock
    for (int i = 0; i < num_stocks; i++) {
        printf("Running Monte Carlo simulation for %s...\n", stocks[i].ticker);
        run_monte_carlo(&stocks[i], output, &config);
    }
    
    fclose(output);
    
    printf("\nAnalysis complete! Results written to %s\n", config.output_file);
    printf("Check the output file for detailed statistics, graphs, and risk metrics.\n");
    
    free(stocks);
    return 0;
}