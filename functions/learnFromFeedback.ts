import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const user = await base44.auth.me();
    
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { feedbackId, updateId, title, summary, source, domain, isRelevant, reason, details, includeKeywords = [] } = await req.json();

    // Handle RELEVANT articles - learn inclusion patterns
    if (isRelevant) {
      const rulesToCreate = [];
      
      // Create include rules from user-provided keywords
      for (const keyword of includeKeywords) {
        rulesToCreate.push({
          rule_type: "include_keyword",
          pattern: keyword.toLowerCase(),
          domain: domain,
          reason: `User marked article as relevant: ${reason}`,
          accuracy_score: 0.8
        });
      }

      // Use AI to extract additional inclusion patterns
      const includeResult = await base44.integrations.Core.InvokeLLM({
        prompt: `You are an AI learning system. A user marked this article as RELEVANT to their SaaS compliance needs.

ARTICLE:
- Title: ${title}
- Source: ${source}
- Domain: ${domain}
- Summary: ${summary || 'Not provided'}

USER FEEDBACK:
- Reason: ${reason}
- User-provided keywords to include: ${includeKeywords.join(', ') || 'None'}

Extract 2-3 additional specific keywords or topics from this article that should be PRIORITIZED in future scans. Be conservative - only suggest patterns clearly indicated by this article.`,
        response_json_schema: {
          type: "object",
          properties: {
            include_keywords: { type: "array", items: { type: "string" } },
            include_topics: { type: "array", items: { type: "string" } },
            confidence: { type: "number" }
          }
        }
      });

      if (includeResult.include_keywords?.length > 0) {
        for (const kw of includeResult.include_keywords) {
          rulesToCreate.push({
            rule_type: "include_keyword",
            pattern: kw.toLowerCase(),
            domain: domain,
            reason: `AI learned from relevant article: ${title.substring(0, 50)}`,
            accuracy_score: includeResult.confidence || 0.7
          });
        }
      }

      if (includeResult.include_topics?.length > 0) {
        for (const topic of includeResult.include_topics) {
          rulesToCreate.push({
            rule_type: "include_topic",
            pattern: topic.toLowerCase(),
            domain: domain,
            reason: `AI learned from relevant article: ${title.substring(0, 50)}`,
            accuracy_score: includeResult.confidence || 0.7
          });
        }
      }

      // Save rules
      let rulesCreated = 0;
      let rulesUpdated = 0;
      const allPatterns = [];

      for (const rule of rulesToCreate) {
        allPatterns.push(rule.pattern);
        const existing = await base44.asServiceRole.entities.RelevanceRule.filter({
          rule_type: rule.rule_type,
          pattern: rule.pattern
        });

        if (existing.length > 0) {
          await base44.asServiceRole.entities.RelevanceRule.update(existing[0].id, {
            derived_from_feedback_count: (existing[0].derived_from_feedback_count || 1) + 1,
            accuracy_score: Math.min(1, (existing[0].accuracy_score || 0.7) + 0.05)
          });
          rulesUpdated++;
        } else {
          await base44.asServiceRole.entities.RelevanceRule.create(rule);
          rulesCreated++;
        }
      }

      // Update feedback record
      await base44.asServiceRole.entities.RelevanceFeedback.update(feedbackId, {
        learned_patterns: allPatterns,
        confidence_score: includeResult.confidence || 0.7
      });

      return Response.json({
        success: true,
        patterns_learned: rulesCreated + rulesUpdated,
        rules_created: rulesCreated,
        rules_updated: rulesUpdated,
        patterns: allPatterns,
        type: "include"
      });
    }

    // Handle IRRELEVANT articles - learn exclusion patterns
    // Use AI to extract learnable patterns from the feedback
    const llmResult = await base44.integrations.Core.InvokeLLM({
      prompt: `You are an AI learning system for a regulatory compliance tool. A user has marked the following article as NOT RELEVANT to their SaaS business compliance needs.

ARTICLE DETAILS:
- Title: ${title}
- Source: ${source}
- Domain: ${domain}
- Summary: ${summary || 'Not provided'}

USER FEEDBACK:
- Reason: ${reason}
- Additional Details: ${details || 'Not provided'}

Analyze this feedback and extract patterns that should be used to filter out similar irrelevant articles in future scans. Consider:

1. TOPIC EXCLUSIONS: Specific topics within the domain that aren't relevant (e.g., "grocery mergers" within Antitrust)
2. KEYWORD PATTERNS: Words or phrases that indicate irrelevance (be specific, avoid overly broad terms)
3. SOURCE PATTERNS: Any source-specific patterns to watch for
4. TITLE PATTERNS: Common title structures that indicate irrelevant content

Be conservative - only suggest patterns that are clearly indicated by this feedback. Don't over-generalize.
Each pattern should be specific enough to avoid false positives but general enough to catch similar articles.`,
      response_json_schema: {
        type: "object",
        properties: {
          topic_exclusions: {
            type: "array",
            items: { type: "string" },
            description: "Specific sub-topics to exclude"
          },
          keyword_patterns: {
            type: "array",
            items: { type: "string" },
            description: "Keywords indicating irrelevance"
          },
          title_patterns: {
            type: "array",
            items: { type: "string" },
            description: "Title patterns to filter"
          },
          confidence: {
            type: "number",
            description: "Confidence in these patterns (0-1)"
          },
          reasoning: {
            type: "string",
            description: "Explanation of the learning"
          }
        }
      }
    });

    const patterns = llmResult;
    const allPatterns = [];
    
    // Create relevance rules from extracted patterns
    const rulesToCreate = [];

    // Topic exclusions
    if (patterns.topic_exclusions?.length > 0) {
      for (const topic of patterns.topic_exclusions) {
        rulesToCreate.push({
          rule_type: "exclude_topic",
          pattern: topic.toLowerCase(),
          domain: domain,
          reason: `Learned from feedback: ${reason}`,
          accuracy_score: patterns.confidence || 0.7
        });
        allPatterns.push(topic);
      }
    }

    // Keyword patterns
    if (patterns.keyword_patterns?.length > 0) {
      for (const keyword of patterns.keyword_patterns) {
        rulesToCreate.push({
          rule_type: "exclude_keyword",
          pattern: keyword.toLowerCase(),
          domain: domain,
          source_name: source,
          reason: `Learned from feedback: ${reason}`,
          accuracy_score: patterns.confidence || 0.7
        });
        allPatterns.push(keyword);
      }
    }

    // Title patterns
    if (patterns.title_patterns?.length > 0) {
      for (const titlePattern of patterns.title_patterns) {
        rulesToCreate.push({
          rule_type: "exclude_title_pattern",
          pattern: titlePattern.toLowerCase(),
          domain: domain,
          reason: `Learned from feedback: ${reason}`,
          accuracy_score: patterns.confidence || 0.7
        });
        allPatterns.push(titlePattern);
      }
    }

    // Check for existing similar rules and update or create
    let rulesCreated = 0;
    let rulesUpdated = 0;

    for (const rule of rulesToCreate) {
      // Check if similar rule exists
      const existingRules = await base44.asServiceRole.entities.RelevanceRule.filter({
        rule_type: rule.rule_type,
        pattern: rule.pattern
      });

      if (existingRules.length > 0) {
        // Update existing rule - increase confidence
        const existing = existingRules[0];
        await base44.asServiceRole.entities.RelevanceRule.update(existing.id, {
          derived_from_feedback_count: (existing.derived_from_feedback_count || 1) + 1,
          accuracy_score: Math.min(1, (existing.accuracy_score || 0.7) + 0.05)
        });
        rulesUpdated++;
      } else {
        // Create new rule
        await base44.asServiceRole.entities.RelevanceRule.create(rule);
        rulesCreated++;
      }
    }

    // Update the feedback record with learned patterns
    await base44.asServiceRole.entities.RelevanceFeedback.update(feedbackId, {
      learned_patterns: allPatterns,
      topic_exclusions: patterns.topic_exclusions || [],
      confidence_score: patterns.confidence || 0.7
    });

    // Optionally hide the update from dashboard
    await base44.asServiceRole.entities.RegulatoryUpdate.update(updateId, {
      status: "Resolved"
    });

    return Response.json({
      success: true,
      patterns_learned: rulesCreated + rulesUpdated,
      rules_created: rulesCreated,
      rules_updated: rulesUpdated,
      patterns: allPatterns,
      reasoning: patterns.reasoning
    });

  } catch (error) {
    console.error("Learning error:", error);
    return Response.json({ error: error.message }, { status: 500 });
  }
});