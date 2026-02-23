## 0221

### research
codex/claude, spawn a agent team to investigate solution to native integration trino with opensearch
- investigate trino key compoent of build a distributed query engine
- investigate how trion integrate with opensearch using connector
- **native opensearch integration** means bring trino key commponents sql parse, ast, optimizer, physical operator, functions, scheduler, shuffle code into opensearch.
- do not consider any existing module in current opensearch sql plugin, let's define a new _sql rest api to support trino sql
- focus on technology challenge
- put results in docs/research/finding_codex.md

### review
Spawn a agent team, review design docs/research/finding_codex.md from different angle, put results in docs/research/review_claude.md
Spawn a agent team, review design docs/research/finding_claude.md from different angle, put results in docs/research/review_codex.md

### design
Proposal a design of opensearch distributed engined (dqe), store in docs/design/dqe_design.md. Read docs/research/finding_claude.md, Address comments from docs/research/review_codex.md. 
- **RULE** DO NOT USE EXISTING SQL AND PPL IMPLEMENTATION, DO NOT CONSIDER EXISTING CALCITE IMPLEMENTATION