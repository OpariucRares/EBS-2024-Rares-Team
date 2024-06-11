## Technical report
Setup:

- Physical machine hardware: AMD Ryzen 7 7840HS with Radeon 780M Graphics.
- Virtual machine hardware: 4 CPUs, 16GB RAM.
- Number of publications: up to 10000 publications (available to be transmitted).
- Number of subsctipions:
  - Subscriber 1: 5000 subscriptions.
  - Subscriber 2: 5000 subscriptions.
 
- Feed time: 3 minutes.
- Subscription parameters:
  - Field distribution: company: 50%, value: 20%, drop: 10%, variation: 10%, date: 10%.
  - Equality operator probability on company field: 25% vs. 100%.
 
**Results**:

| Metric                                | 25% equality on company       | 100% equality on company             |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (171 out of 171)         | 100% (171 out of 171)                |
| Mean latency                          | 18 / 24 ms                    | 15 / 31 ms                           |
| Matching rate                         | 99.41% (170 / 170 out of 171) | 97.66% / 100% (167 / 171 out of 171) |

## Topology

![image](https://github.com/OpariucRares/EBS-2024-Rares-Team/assets/25665859/32aff337-f3da-41a5-86ba-17b7be7ce5aa)

