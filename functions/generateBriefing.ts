import { createClientFromRequest } from 'npm:@base44/sdk@0.8.4';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    
    const user = await base44.auth.me();
    if (!user) {
      return Response.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { configId } = await req.json();
    
    if (!configId) {
      return Response.json({ error: 'configId is required' }, { status: 400 });
    }

    // Fetch the briefing configuration
    const configs = await base44.asServiceRole.entities.BriefingConfig.filter({ id: configId });
    if (configs.length === 0) {
      return Response.json({ error: 'Configuration not found' }, { status: 404 });
    }
    const config = configs[0];

    // Calculate date range based on frequency
    const now = new Date();
    let periodStart = new Date();
    
    switch (config.frequency) {
      case 'daily':
        periodStart.setDate(now.getDate() - 1);
        break;
      case 'weekly':
        periodStart.setDate(now.getDate() - 7);
        break;
      case 'monthly':
        periodStart.setMonth(now.getMonth() - 1);
        break;
    }

    // Fetch relevant regulatory updates
    const allUpdates = await base44.asServiceRole.entities.RegulatoryUpdate.filter({});
    
    // Filter updates based on config
    const filteredUpdates = allUpdates.filter(update => {
      const updateDate = new Date(update.publish_date || update.created_date);
      if (updateDate < periodStart) return false;
      
      if (config.domains?.length > 0 && !config.domains.includes(update.domain)) return false;
      if (config.risk_levels?.length > 0 && !config.risk_levels.includes(update.risk_score)) return false;
      if (config.jurisdictions?.length > 0 && !config.jurisdictions.includes(update.jurisdiction)) return false;
      
      return true;
    });

    // Sort by risk (High first) then by date
    filteredUpdates.sort((a, b) => {
      const riskOrder = { High: 0, Medium: 1, Low: 2 };
      if (riskOrder[a.risk_score] !== riskOrder[b.risk_score]) {
        return riskOrder[a.risk_score] - riskOrder[b.risk_score];
      }
      return new Date(b.publish_date || b.created_date) - new Date(a.publish_date || a.created_date);
    });

    // Generate briefing content using LLM
    const updatesText = filteredUpdates.map(u => 
      `Title: ${u.title}\nSource URL: ${u.source_url || 'N/A'}\nDomain: ${u.domain}\nJurisdiction: ${u.jurisdiction}\nRisk: ${u.risk_score}\nType: ${u.update_type}\nSummary: ${u.summary}\nCompliance Actions: ${u.compliance_actions?.join('; ') || 'N/A'}\nKey Dates: ${u.key_dates?.join('; ') || 'N/A'}`
    ).join('\n\n---\n\n');

    const llmResult = await base44.integrations.Core.InvokeLLM({
      prompt: `You are a senior legal compliance analyst. Generate a professional legal briefing based on the following regulatory updates.

BRIEFING CONFIGURATION:
- Name: ${config.name}
- Frequency: ${config.frequency}
- Domains: ${config.domains?.join(', ')}
- Risk Levels: ${config.risk_levels?.join(', ')}
- Period: ${periodStart.toISOString().split('T')[0]} to ${now.toISOString().split('T')[0]}
- Include Executive Summary: ${config.include_executive_summary}
- Include Action Items: ${config.include_action_items}

REGULATORY UPDATES (${filteredUpdates.length} total):
${updatesText || 'No updates found for this period.'}

Generate a well-structured HTML briefing with:
1. ${config.include_executive_summary ? 'An executive summary (2-3 paragraphs) highlighting the most critical developments' : ''}

2. **SENTIMENT ANALYSIS SECTION**: Analyze the overall regulatory sentiment/tone:
   - Is the regulatory environment becoming more restrictive or permissive?
   - What is the general direction of enforcement activity?
   - Display with a visual indicator (e.g., 🔴 Restrictive, 🟡 Neutral, 🟢 Permissive)
   - Include a brief explanation of the trend

3. **POTENTIAL CONFLICTS & INCONSISTENCIES**: Identify any conflicts or tensions between different regulatory updates or sources:
   - Conflicting guidance from different agencies
   - Jurisdictional conflicts (e.g., US vs EU approaches)
   - Updates that may contradict each other
   - Areas where compliance with one regulation may conflict with another
   - Display each conflict with clear explanation and recommendation

4. Updates organized by domain, with each update including:
   - Title as a clickable link to the source URL (use <a href="SOURCE_URL" target="_blank" style="color: #3b82f6; text-decoration: none;">Title</a>)
   - Risk level badge
   - Sentiment indicator for this specific update (🔴 Negative/Restrictive, 🟡 Neutral, 🟢 Positive/Permissive)
   - Key points
   - ${config.include_action_items ? 'Specific action items for compliance teams' : ''}

5. ${config.include_action_items ? 'A consolidated action items section at the end' : ''}

6. Professional formatting with clear headers and visual hierarchy

IMPORTANT: Make each article title a clickable hyperlink to its source URL so readers can access the original article.

Use inline CSS for styling (no external stylesheets). Use a clean, professional color scheme. Style the sentiment and conflicts sections with appropriate colors and borders to make them stand out.`,
      response_json_schema: {
        type: "object",
        properties: {
          executive_summary: { type: "string" },
          html_content: { type: "string" },
          overall_sentiment: {
            type: "string",
            enum: ["restrictive", "neutral", "permissive"]
          },
          sentiment_explanation: { type: "string" },
          conflicts: {
            type: "array",
            items: {
              type: "object",
              properties: {
                title: { type: "string" },
                description: { type: "string" },
                affected_sources: { type: "array", items: { type: "string" } },
                recommendation: { type: "string" }
              }
            }
          },
          key_action_items: { 
            type: "array", 
            items: { type: "string" } 
          }
        },
        required: ["html_content"]
      }
    });

    // Create briefing title
    const briefingTitle = `${config.name} - ${now.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}`;

    // Save the briefing to archive
    const briefing = await base44.asServiceRole.entities.Briefing.create({
      title: briefingTitle,
      frequency: config.frequency,
      domains: config.domains,
      risk_levels: config.risk_levels,
      content: llmResult.html_content,
      summary: llmResult.executive_summary || '',
      updates_count: filteredUpdates.length,
      period_start: periodStart.toISOString().split('T')[0],
      period_end: now.toISOString().split('T')[0],
      sent_to: config.recipients,
      status: "generated"
    });

    // Send emails to recipients
    const emailErrors = [];
    for (const recipient of config.recipients || []) {
      try {
        await base44.integrations.Core.SendEmail({
          to: recipient,
          subject: `Legal Briefing: ${briefingTitle}`,
          body: `
            <html>
              <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto;">
                <div style="background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%); color: white; padding: 24px; border-radius: 8px 8px 0 0;">
                  <h1 style="margin: 0; font-size: 24px;">${briefingTitle}</h1>
                  <p style="margin: 8px 0 0 0; opacity: 0.9;">
                    ${config.frequency.charAt(0).toUpperCase() + config.frequency.slice(1)} Legal Briefing • ${filteredUpdates.length} Updates
                  </p>
                </div>
                ${llmResult.html_content}
                <div style="background: #f8fafc; padding: 24px; border-radius: 0 0 8px 8px; text-align: center;">
                  <p style="color: #64748b; font-size: 12px; margin: 0 0 12px 0;">
                    Generated by ComplianceAI • ${now.toLocaleDateString()}
                  </p>
                  <a href="https://base44.com" target="_blank" style="display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%); color: white; padding: 10px 24px; border-radius: 6px; text-decoration: none; font-weight: 500; font-size: 14px;">
                    Open ComplianceAI Dashboard →
                  </a>
                </div>
              </body>
            </html>
          `
        });
      } catch (err) {
        emailErrors.push(`Failed to send to ${recipient}: ${err.message}`);
      }
    }

    // Update briefing status and config
    await base44.asServiceRole.entities.Briefing.update(briefing.id, {
      status: emailErrors.length === 0 ? "sent" : (emailErrors.length < config.recipients.length ? "sent" : "failed")
    });

    await base44.asServiceRole.entities.BriefingConfig.update(config.id, {
      last_generated: now.toISOString()
    });

    return Response.json({
      success: true,
      briefingId: briefing.id,
      updatesIncluded: filteredUpdates.length,
      emailsSent: config.recipients.length - emailErrors.length,
      emailErrors: emailErrors.length > 0 ? emailErrors : undefined
    });

  } catch (error) {
    return Response.json({ error: error.message }, { status: 500 });
  }
});