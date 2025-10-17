#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#define LINE_BUF 1048576
#define MAX_COLS 8192

typedef enum { TYPE_UNKNOWN=0, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } ColType;

static int is_integer(const char *s){
    // Returns true if the string is a valid integer with optional sign and
    // surrounding whitespace. Rejects empty and partial matches
    const char *p=s; 
    while(isspace((unsigned char)*p)) p++;
    if(*p=='+'||*p=='-') p++;
    int dig=0; 
    while(isdigit((unsigned char)*p)) {
        dig=1;p++;
    }
    while(isspace((unsigned char)*p)) p++;
    return dig && *p=='\0';
}

static int is_real(const char *s){
    // Returns true if the string is a valid real (floating-point) literal with
    // optional sign, optional fractional part, and optional scientific exponen
    // Surrounding whitespace is also ignored
    const char *p=s; 
    while(isspace((unsigned char)*p)) p++;
    if(*p=='+'||*p=='-') p++;
    int dig=0;

    while(isdigit((unsigned char)*p)) {
        dig=1;
        p++;}

    if(*p=='.') {
        p++; 
        while(isdigit((unsigned char)*p)){dig=1;p++;}
    }

    if(!dig) return 0;

    if(*p=='e'||*p=='E') { 
        p++; 
        if(*p=='+'||*p=='-') p++; 
        int expd=0; 
        while(isdigit((unsigned char)*p)) {
            expd=1;p++;
        } 
        if(!expd) return 0; 
    }
    while(isspace((unsigned char)*p)) p++;
    return *p=='\0';
}

static void chomp(char *s){
    // In-place strip of trailing CR/LF from a line buffer.
    size_t n=strlen(s);
    while(n && (s[n-1]=='\n' || s[n-1]=='\r')) s[--n]=0;
}

// Delimiter-aware CSV split with quotes
static int split_csv_line(char *line, char *cells[], int max_cells, char delim){
    // Splits a single CSV record in-place using the given delimiter (for now always ',')
    // - respects double-quoted fields, including doubled quotes for escaping
    // - writes NUL terminators to separate fields; returns number of cells
    // - does not support fields spanning multiple physical lines (might add)
    int count=0;
    char *p=line;
    while(*p && count<max_cells){
        char *start=p;
        if(*p=='"'){
            p++; // skip opening quote
            char *w=start; // overwrite in-place, start will point to content
            for(;;){
                if(*p=='"'){
                    if(*(p+1)=='"'){ *w++='"'; p+=2; }
                    else{ p++; break; } // end quote
                } else if(*p=='\0'){ break; }
                else{ *w++=*p++; }
            }
            *w='\0';
            // move to delimiter or end
            while(*p && *p!=delim) p++;
            cells[count++]=start+1; // +1 because start had the opening quote
            if(*p==delim) p++;
        } else {
            // Non-quoted field: do NOT copy byte-by-byte in place, because
            // writing the terminating NUL at 'w' can overwrite the delimiter
            // (since p and w move in lockstep). Instead, find the delimiter,
            // NUL-terminate at the delimiter itself, then advance past it
            char *start = p;
            while (*p && *p != delim) p++;
            if (*p == delim) {
                *p = '\0';
                cells[count++] = start;
                p++; // skip delimiter
            } else {
                // end of line
                cells[count++] = start;
            }
        }
    }
    return count;
}

static void strip_bom(char *s){
    // If the buffer starts with UTF-8 BOM, drop it in place
    unsigned char *u=(unsigned char*)s;
    if(u[0]==0xEF && u[1]==0xBB && u[2]==0xBF) {
        memmove(s, s+3, strlen(s+3)+1);
    }
}

// Delimiter is fixed to comma; no auto-detection

static void print_json(char *headers[], ColType types[], int n){
    // Minimal JSON writer for the result. Escapes quotes and backslashes in
    // header names. Types are serialized as strings compatible with SQLite
    printf("{\"columns\":[");
    for(int i=0;i<n;i++) {
        if(i) printf(",");
        printf("\"");
        for(const char *s=headers[i]; *s; s++){
            if(*s=='\\'||*s=='"') putchar('\\');
            putchar(*s);
        }
        printf("\"");
    }
    printf("],\"types\":[");
    for(int i=0;i<n;i++) {
        const char *t="TEXT";
        if(types[i]==TYPE_INTEGER) t="INTEGER";
        else if(types[i]==TYPE_REAL) t="REAL";
        if(i) printf(",");
        printf("\"%s\"", t);
    }
    printf("]}");
}

int main(int argc, char **argv){
    // CLI expects exactly one positional argument: the path to a CSV file
    // Outputs a single JSON object to stdout; errors and usage to stderr
    if(argc!=2){
        fprintf(stderr,"usage: %s <csv_path>\n", argv[0]);
        return 2;
    }

    const char *path=argv[1];
    FILE *f=fopen(path,"rb");

    if(!f) {
        fprintf(stderr,"error: %s: %s\n", path, strerror(errno)); 
        return 1; 
    }

    static char line[LINE_BUF];
    static char *cells[MAX_COLS];
    static char *headers[MAX_COLS];
    ColType types[MAX_COLS];

    // Read header (first line) and split into column names
    if(!fgets(line, sizeof(line), f)) { 
        printf("{\"columns\":[],\"types\":[]}"); 
        fclose(f); 
        return 0; }

    strip_bom(line);
    chomp(line);
    char delim = ','; // fixed comma delimiter
    int ncols = split_csv_line(line, cells, MAX_COLS, delim);

    if(ncols<=0) { 
        printf("{\"columns\":[],\"types\":[]}"); fclose(f); return 0; }

    for(int i=0;i<ncols;i++) { 
        headers[i]=strdup(cells[i] ? cells[i] : "");
        types[i]=TYPE_UNKNOWN;
    }

    // Scan data rows, upgrading type per column as evidence appears
    while(fgets(line, sizeof(line), f)) {
        chomp(line);
        int c = split_csv_line(line, cells, MAX_COLS, delim);
        // pad missing with empty strings (short rows)
        for(int i=c;i<ncols;i++) cells[i]="";

        for(int i=0;i<ncols;i++){
            const char *cell = cells[i] ? cells[i] : "";
            if(!*cell) continue; // empty doesn't upgrade type
            if(types[i]==TYPE_TEXT) continue;

            if(is_integer(cell)){
                if(types[i]==TYPE_UNKNOWN) types[i]=TYPE_INTEGER;
                // keep REAL if already REAL
            } else if(is_real(cell)){
                if(types[i]==TYPE_UNKNOWN || types[i]==TYPE_INTEGER) types[i]=TYPE_REAL;
            } else {
                types[i]=TYPE_TEXT;
            }
        }
    }
    fclose(f);

    // Default any remaining unknown columns to TEXT
    for(int i=0;i<ncols;i++) if(types[i]==TYPE_UNKNOWN) types[i]=TYPE_TEXT;

    print_json(headers, types, ncols);
    for(int i=0;i<ncols;i++) free(headers[i]);
    return 0;
}
