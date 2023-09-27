# Distributed Systems Challenge - Fly.io

This repository is my attempt at solving every problem in the distributed systems challenge from the Fly.io company: https://fly.io/dist-sys/

Note that this is not job related or anything of the sort, it is more like a public challenge and everyone should be able to read the specs and try out their own implementations.

## Overview of The Solutions

1. Echo

Works fine, a lot of work was done to get Maelstrom up and running after reading the docs.

2. Unique Id

Works fine, with Maelstrom running OK, this was a rather simple challenge.

3. Broadcast

This gave me a lot of trouble, specially with not flooding the channel with messages and prioritizing "customer" related requests.

4. Increasing Counter

This is also working for a small number of nodes, the challenge specifies 3, but increasing the node count will likely slowdown my solution.

5. Kafka Style Replicated Log

Work in Progress.