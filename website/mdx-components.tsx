import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { Terminal } from "@/components/kafka/terminal";
import { KafkaPartitions } from "@/components/kafka/kafka-partitions";
import { ConsumerLag } from "@/components/kafka/consumer-lag";
import { ScenarioFlow } from "@/components/kafka/scenario-flow";
import { Callout } from "fumadocs-ui/components/callout";
import { Tab, Tabs } from "fumadocs-ui/components/tabs";
import { Step, Steps } from "fumadocs-ui/components/steps";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Terminal,
    KafkaPartitions,
    ConsumerLag,
    ScenarioFlow,
    Callout,
    Tab,
    Tabs,
    Step,
    Steps,
    ...components,
  };
}
