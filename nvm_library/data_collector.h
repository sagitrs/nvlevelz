#ifndef DATA_COLLECTOR_H
#define DATA_COLLECTOR_H
#include "global.h"
namespace leveldb {

struct DataCollector {
    enum {Second = 1000000, Minute = 60000000};
    double Chaos_Plus, Chaos_Minus[16];
    ull Mem_Write_Time;
    ull Log_Write_Time;
    ull Compaction_Time;
    ull SiniorCompaction_Time;
    ull MajorCompaction_Time;
    ull Mem_Read_Time;
    ull Disk_Read_Time;
    ull Sinior_Compaction[6];
    ull Major_Compaction[12];
    ull Major_Compaction_Level[16];
    ull locate_[4];
    ull Total_Comaction;

    ull Global_Time;
    ull Report_Boundary;
    DataCollector() :
        Chaos_Plus(0),
        Mem_Write_Time(0), Log_Write_Time(0),
        Compaction_Time(0), SiniorCompaction_Time(0), MajorCompaction_Time(0),
        Mem_Read_Time(0),Disk_Read_Time(0),Total_Comaction(0),
        Global_Time(0), Report_Boundary(1 * Minute) {
        for (int i = 0; i < 6; ++i)
            Sinior_Compaction[i] = 0;
        for (int i = 0; i < 12; ++i)
            Major_Compaction[i] = 0;
        for (int i = 0; i < 16; ++i) {
            Major_Compaction_Level[i] = 0;
            Chaos_Minus[i] = 0;
        }
        for (int i = 0; i < 4; ++i)
            locate_[i] = 0;
    }
    void AddGlobalTime(ull t) {
        Global_Time += t;
        if (Global_Time >= Report_Boundary) {
            Report_Boundary += 1 * Minute;
            double rmem = 100.0 * Mem_Read_Time / Global_Time;
            double rdisk = 100.0 * Disk_Read_Time / Global_Time;
            double wmem = 100.0 * Mem_Write_Time / Global_Time;
            double wlog = 100.0 * Log_Write_Time / Global_Time;
            double wcomp = 100.0 * Compaction_Time / Global_Time;
            double comps = 100.0* SiniorCompaction_Time / Global_Time;
            double compm = 100.0* MajorCompaction_Time / Global_Time;

            printf("Time = %llu(s)\n", Global_Time / Minute);
            printf("Read = %.2f%% = %.2f%% in Mem + %.2f%% in Disk\n",
                   rmem+rdisk, rmem, rdisk);
            double total = locate_[0] + locate_[1] + locate_[2] + locate_[3];
            printf("Read Type = %.2f(MEM) + %.2f(IMM) + %.2f(DISK) _ %.2f NOTHIT\n",
                   locate_[0] / total, locate_[1] / total, locate_[2] / total, locate_[3] / total);
            printf("Write = %.2f%% = %.2f%% in Mem + %.2f%% in Log + %.2f%% in Compact\n",
                   wmem+wlog+wcomp, wmem, wlog, wcomp);
            printf("Compaction Time = %.2f%% = %.2f%% Sinior + %.2f%% Major\n",
                   wcomp, comps, compm);

            ull total_sinior = 0, total_major = 0;
            for (int i = 0; i < 6; ++i)
                total_sinior += Sinior_Compaction[i];
            for (int i = 1; i < 12; ++i)
                total_major += Major_Compaction[i];
            printf("Compaction = %llu = %lld Sinior + %llu Major\n",
                   Total_Comaction, total_sinior, total_major);
            printf("Sinior = %llu\n", total_sinior);
            for (int i = 0; i < 6; ++i)
                printf("%d = %.2f%%,\t", i, 100.0 * Sinior_Compaction[i] / total_sinior);
            printf("\n");
            printf("Trivival Compaction = %llu\n", Major_Compaction[0]);
            printf("Major by files = %llu\n ", total_major);
            for (int i = 1; i < 12; ++i)
                printf("%d = %.2f%%,\t", i, 100.0* Major_Compaction[i] / total_major);
            printf("\n");
            printf("Major by level = %llu\n ", total_major);
            for (int i = 0; i < 10; ++i)
                printf("%d = %.2f%%,\t", i, 100.0* Major_Compaction_Level[i] / total_major);
            printf("\n");
            double Chaos_Minus_Total = 0;
            for (int i = 0; i < 6; ++i) {
                printf("%d = %.2f,\t", i, Chaos_Minus[i]);
                Chaos_Minus_Total += Chaos_Minus[i];
            }
            printf("\n");
            printf("Chaos = %.2f = %.2f - %.2f\n", Chaos_Plus - Chaos_Minus_Total, Chaos_Plus, Chaos_Minus_Total);
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("-------------------------------------------------------");
            printf("\n");
/*
            Global_Time = 0;
            Mem_Read_Time = 0;
            Disk_Read_Time = 0;
            Write_Time = 0;
            Compaction_Time = 0;
            SiniorCompaction_Time = 0;
            MajorCompaction_Time = 0;
            */
        }
    }
    void AddChaos(double t) {
        Chaos_Plus += t;
    }
    void AddLocateType(byte x) {
        locate_[x] ++;
    }
    void MinusChaos(byte level, double t) {
        Chaos_Minus[level] += t;
    }
    void AddLogWriteTime(ull t) {
        Log_Write_Time += t;
        AddGlobalTime(t);
    }
    void AddMemWriteTime(ull t) {
        Mem_Write_Time += t;
        AddGlobalTime(t);
    }
    void AddSiniorCompactionTime(ull t) {
        SiniorCompaction_Time += t;
        Compaction_Time += t;
        AddGlobalTime(t);
    }
    void AddMajorCompactionTime(ull t) {
        MajorCompaction_Time += t;
        Compaction_Time += t;
        AddGlobalTime(t);
    }
    void AddMemReadTime(ull t) {
        Mem_Read_Time += t;
        AddGlobalTime(t);
    }
    void AddDiskReadTime(ull t) {
        Disk_Read_Time += t;
        AddGlobalTime(t);
    }
    void AddSiniorCompaction(size_t level, ull t) {
        Sinior_Compaction[level] += t;
        Total_Comaction += t;
        AddGlobalTime(t);
    }
    void AddMajorCompaction(size_t files, size_t level, ull t) {
        Major_Compaction[files] += t;
        if (files != 0) {
            Total_Comaction += t;
            Major_Compaction_Level[level] += t;
        }

        AddGlobalTime(t);
    }
};

}
#endif // DATA_COLLECTOR_H
