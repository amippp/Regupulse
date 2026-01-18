import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);
    const payload = await req.json();
    
    const { 
      type, 
      title, 
      message, 
      priority = "medium", 
      recipient_email, 
      recipient_emails,
      related_update_id,
      action_url,
      send_email = true 
    } = payload;

    // Get list of recipients
    let recipients = [];
    if (recipient_email) {
      recipients = [recipient_email];
    } else if (recipient_emails && recipient_emails.length > 0) {
      recipients = recipient_emails;
    } else {
      // If no recipient specified, get all users
      const users = await base44.asServiceRole.entities.User.list();
      recipients = users.map(u => u.email);
    }

    const results = {
      notifications_created: 0,
      emails_sent: 0,
      errors: []
    };

    for (const email of recipients) {
      try {
        // Get user's notification preferences
        const prefsResult = await base44.asServiceRole.entities.NotificationPreferences.filter({ 
          user_email: email 
        });
        const prefs = prefsResult[0] || {
          email_enabled: true,
          in_app_enabled: true,
          high_risk_updates: true,
          source_health_alerts: true,
          deadline_reminders: true,
          new_regulations: false,
          email_frequency: "immediate"
        };

        // Check if this notification type is enabled
        const typeEnabled = {
          high_risk_update: prefs.high_risk_updates,
          source_health: prefs.source_health_alerts,
          deadline: prefs.deadline_reminders,
          new_regulation: prefs.new_regulations,
          system: true // Always allow system notifications
        };

        if (!typeEnabled[type] && type !== "system") {
          continue; // Skip if user disabled this type
        }

        // Check quiet hours
        if (prefs.quiet_hours_start !== null && prefs.quiet_hours_end !== null) {
          const now = new Date();
          const currentHour = now.getHours();
          const start = prefs.quiet_hours_start;
          const end = prefs.quiet_hours_end;
          
          const inQuietHours = start < end 
            ? (currentHour >= start && currentHour < end)
            : (currentHour >= start || currentHour < end);
          
          if (inQuietHours && priority !== "critical") {
            // Queue for later - for now, we'll still create the notification but skip email
            prefs.email_enabled = false;
          }
        }

        // Create in-app notification if enabled
        if (prefs.in_app_enabled) {
          await base44.asServiceRole.entities.Notification.create({
            title,
            message,
            type,
            priority,
            recipient_email: email,
            related_update_id,
            action_url,
            is_read: false
          });
          results.notifications_created++;
        }

        // Send email if enabled and frequency is immediate
        if (send_email && prefs.email_enabled && prefs.email_frequency === "immediate") {
          const priorityEmoji = {
            critical: "🚨",
            high: "⚠️",
            medium: "📋",
            low: "ℹ️"
          };

          const emailSubject = `${priorityEmoji[priority] || ""} ${title}`;
          const emailBody = `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <div style="background: linear-gradient(135deg, #3b82f6, #8b5cf6); padding: 24px; border-radius: 12px 12px 0 0;">
                <h1 style="color: white; margin: 0; font-size: 20px;">Compliance Alert</h1>
              </div>
              <div style="padding: 24px; background: #f8fafc; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 12px 12px;">
                <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid #e2e8f0;">
                  <h2 style="margin: 0 0 12px 0; color: #1e293b; font-size: 18px;">${title}</h2>
                  <p style="color: #64748b; line-height: 1.6; margin: 0 0 16px 0;">${message}</p>
                  ${action_url ? `
                    <a href="${action_url}" style="display: inline-block; background: #3b82f6; color: white; padding: 10px 20px; border-radius: 6px; text-decoration: none; font-weight: 500;">
                      View Details →
                    </a>
                  ` : ''}
                </div>
                <p style="color: #94a3b8; font-size: 12px; margin: 16px 0 0 0; text-align: center;">
                  You received this email because you have ${type.replace(/_/g, ' ')} notifications enabled.
                  <br>
                  <a href="#" style="color: #64748b;">Manage notification preferences</a>
                </p>
              </div>
            </div>
          `;

          await base44.asServiceRole.integrations.Core.SendEmail({
            to: email,
            subject: emailSubject,
            body: emailBody
          });
          results.emails_sent++;
        }

      } catch (error) {
        results.errors.push(`Failed for ${email}: ${error.message}`);
      }
    }

    return Response.json({
      success: true,
      ...results
    });

  } catch (error) {
    return Response.json({ 
      success: false, 
      error: error.message 
    }, { status: 500 });
  }
});