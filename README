Information Retrival Course Projects
===

Notes
---
* The first 2 projects are written in GoLang adopting the concurrency pattern to speed up the retrival and ranking processes.
* The whole development environment is running on Docker.
* Use `Docker-compose.yml` in each folder to easily bring up the corresponding test container.
* Use `Dockerfile` in `hw1` folder to build the development environment image.

Facts
---
* Typical project 1 programs I've seen from other classmates, which either don't adopt concurrency pattern or use multithreding in some functions, generally takes 4 mins or more to finish the whole indexing process(85,000 docs) and at least 10 mins to query and to rank all the relavent docs(total 25 queries, each requires computing 5 different retrival models), while this program takes only 50-60 secs and 3 mins to do it respectively.
* The ranker program in hw1 use 70-80% CPU and at most 200M Memmory to finish the whole retrival process.