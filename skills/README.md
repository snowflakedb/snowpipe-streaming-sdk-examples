# Snowpipe Streaming SDK Skills

Cortex Code skills for automating Snowpipe Streaming SDK demos, setup, and operations.

## Available Skills

| Skill | Triggers | Description | Time |
|-------|----------|-------------|------|
| [SSv2 Quickstart](./ssv2-quickstart/) | `ssv2 quickstart`, `try snowpipe streaming` | Zero-to-streaming pipeline: OS detection, RSA keys, Python venv, fake data streaming via default pipe, live Streamlit dashboard | ~5 min |
| [SSv2 AI Webinar](./ssv2-ai-webinar/) | `ssv2 ai webinar`, `ssv2 webinar demo` | Everything in Quickstart plus 30-min background streaming, Semantic View, Cortex Agent for natural-language queries, presenter handoff | ~5 min setup |
| [Custom Kafka Consumer](./custom-kafka-consumer/) | `kafka consumer`, `kafka to snowflake` | Set up, run, and debug a Kafka-to-Snowflake streaming pipeline using the SDK with 1:1 partition-to-channel mapping | ~15 min |

## How to Use

These skills are designed for [Claude Code](https://claude.ai/code) (Cortex Code). To use a skill:

1. Open Claude Code in a project that includes this repository
2. Type a trigger phrase (e.g., `ssv2 quickstart`)
3. Follow the interactive prompts

## Adding a New Skill

1. Create a new directory under `skills/` named after your skill slug
2. Add a `SKILL.md` with YAML frontmatter (`name`, `description` with triggers) and step-by-step instructions
3. Include any templates or supporting files alongside the SKILL.md
4. Add an entry to the table above
