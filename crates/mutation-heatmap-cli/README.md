# Mutation Visualizer CLI

## Test Data

Large data for stress testing.

```bash
head -n 1 data/sars-cov-2/nextclade.tsv > data/sars-cov-2/nextclade_big.tsv
for i in $(seq 1 10000); do 
    head -n 4 data/sars-cov-2/nextclade.tsv | tail -n 1 | ./csvtk replace -t -H -f 1 -p "(.*)" -r "Sample$i";
done | cat >> data/sars-cov-2/nextclade_big.tsv
```