## Technical report
Setup:

- Physical machine hardware: AMD Ryzen 7 7840HS with Radeon 780M Graphics.
- Virtual machine hardware: 4 CPUs, 16GB RAM.
- Number of publications: up to 2000 publications (available to be transmitted).
- Number of subsctipions:
  - Subscriber 1: 50 subscriptions.
  - Subscriber 2: 50 subscriptions.
 
- Feed time: 3 minutes.
- Subscription parameters:
  - Field distribution: company: 80%, value: 80%, drop: 80%, variation: 80%, date: 80%.
  - Equality operator probability on company field: 25% vs. 100%.
 
**Results**:

| Metric                                | 25% equality on company       | 100% equality on company             |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (173 out of 173)         | 100% (171 out of 171)                |
| Mean latency                          | 15 / 7 ms                     | 13 / 31 ms                           |
| Matching rate (Mean)                  | 19.94% (2 / 67 out of 173)    | 48.83% (97 / 70 out of 171)          |


| Metric                                | 25% equality on drop          | 100% equality on drop                |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (170 out of 170)         | 100% (170 out of 170)                |
| Mean latency                          | 8 / 9 ms                      | 11 / 14 ms                           |
| Matching rate (Mean)                  | 25.29% (66 / 20 out of 170)   | 0.59% (1 / 1 out of 170)             |


| Metric                                | 25% equality on value         | 100% equality on value               |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (174 out of 174)         | 100% (172 out of 172)                |
| Mean latency                          | 7 / 21 ms                     | 12 / 9 ms                            |
| Matching rate (Mean)                  | 22.98% (37 / 43 out of 172)   | 12.79% (2 / 42 out of 172)           |


| Metric                                | 25% equality on variation     | 100% equality on variation           |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (177 out of 177)         | 100% (177 out of 177)                |
| Mean latency                          | 6 / 7 ms                      | 8 / 13 ms                            |
| Matching rate (Mean)                  | 11.30% (20 / 20 out of 177)   | 2.54% (5 / 4 out of 177)             |


| Metric                                | 25% equality on date          | 100% equality on date                |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (176 out of 176)         | 100% (176 out of 176)                |
| Mean latency                          | 12 / 12 ms                    | 8 / 10 ms                            |
| Matching rate (Mean)                  | 13.92% (23 / 26 out of 176)   | 7.38% (6 / 20 out of 176)            |

## Topology

![image](https://github.com/OpariucRares/EBS-2024-Rares-Team/assets/46899212/7b2ad75c-73b0-470b-9dac-f9403c79fc3a)

)

