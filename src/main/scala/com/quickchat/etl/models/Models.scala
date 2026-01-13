package com.quickchat.etl.models

import java.sql.Timestamp

// ============================================
// BRONZE LAYER MODELS (Raw Data)
// ============================================

case class BronzeMessage(
    _id: String,
    conversationId: String,
    senderId: String,
    messageType: String, // text, image, video, audio, file, call, system
    content: Option[String],
    attachments: Option[Seq[Attachment]],
    reactions: Option[Seq[Reaction]],
    replyTo: Option[String],
    isEdited: Boolean,
    isDeleted: Boolean,
    isForwarded: Boolean,
    readBy: Option[Seq[String]],
    createdAt: Timestamp,
    updatedAt: Timestamp,
    expiresAt: Option[Timestamp],
    // Metadata for partitioning
    ingestTimestamp: Timestamp,
    sourceSystem: String
)

case class Attachment(
    `type`: String,
    url: String,
    filename: Option[String],
    size: Option[Long],
    mimeType: Option[String],
    thumbnailUrl: Option[String]
)

case class Reaction(
    userId: String,
    emoji: String,
    createdAt: Timestamp
)

case class BronzeUser(
    _id: String,
    phoneNumber: Option[String],
    email: Option[String],
    username: Option[String],
    displayName: Option[String],
    avatar: Option[String],
    bio: Option[String],
    status: Option[String],
    lastSeen: Option[Timestamp],
    isOnline: Boolean,
    role: String,
    isBanned: Boolean,
    isVerified: Boolean,
    createdAt: Timestamp,
    updatedAt: Timestamp,
    ingestTimestamp: Timestamp,
    sourceSystem: String
)

case class BronzeConversation(
    _id: String,
    conversationType: String, // single, group
    name: Option[String],
    avatar: Option[String],
    participants: Seq[String],
    admins: Option[Seq[String]],
    lastMessageId: Option[String],
    lastMessageAt: Option[Timestamp],
    isArchived: Boolean,
    isLocked: Boolean,
    createdBy: String,
    createdAt: Timestamp,
    updatedAt: Timestamp,
    ingestTimestamp: Timestamp,
    sourceSystem: String
)

case class BronzeCall(
    _id: String,
    initiatorId: String,
    participantIds: Seq[String],
    conversationId: Option[String],
    callType: String, // audio, video
    status: String, // initiated, ongoing, ended, missed
    startTime: Option[Timestamp],
    endTime: Option[Timestamp],
    duration: Option[Long], // seconds
    createdAt: Timestamp,
    ingestTimestamp: Timestamp,
    sourceSystem: String
)

case class BronzeEvent(
    eventId: String,
    eventType: String,
    entityType: String,
    entityId: String,
    userId: Option[String],
    payload: String, // JSON string
    metadata: Option[Map[String, String]],
    timestamp: Timestamp,
    ingestTimestamp: Timestamp,
    sourceSystem: String
)

// ============================================
// SILVER LAYER MODELS (Cleaned & Enriched)
// ============================================

case class SilverMessage(
    messageId: String,
    conversationId: String,
    senderId: String,
    senderName: Option[String],
    messageType: String,
    content: Option[String],
    contentLength: Option[Int],
    hasAttachments: Boolean,
    attachmentCount: Int,
    attachmentTypes: Seq[String],
    reactionCount: Int,
    uniqueReactors: Int,
    isReply: Boolean,
    replyToMessageId: Option[String],
    isEdited: Boolean,
    isDeleted: Boolean,
    isForwarded: Boolean,
    readCount: Int,
    createdAt: Timestamp,
    updatedAt: Timestamp,
    // Derived time fields for partitioning
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    dayOfWeek: Int,
    isWeekend: Boolean,
    // Processing metadata
    processedAt: Timestamp
)

case class SilverUser(
    userId: String,
    phoneNumber: Option[String],
    email: Option[String],
    username: Option[String],
    displayName: Option[String],
    hasAvatar: Boolean,
    hasBio: Boolean,
    status: Option[String],
    lastSeen: Option[Timestamp],
    lastSeenDaysAgo: Option[Int],
    isOnline: Boolean,
    role: String,
    isBanned: Boolean,
    isVerified: Boolean,
    accountAgeDays: Int,
    createdAt: Timestamp,
    updatedAt: Timestamp,
    year: Int,
    month: Int,
    processedAt: Timestamp
)

case class SilverConversation(
    conversationId: String,
    conversationType: String,
    name: Option[String],
    hasAvatar: Boolean,
    participantCount: Int,
    adminCount: Int,
    hasLastMessage: Boolean,
    lastMessageAt: Option[Timestamp],
    daysSinceLastMessage: Option[Int],
    isArchived: Boolean,
    isLocked: Boolean,
    isActive: Boolean, // has message in last 30 days
    createdBy: String,
    createdAt: Timestamp,
    updatedAt: Timestamp,
    conversationAgeDays: Int,
    year: Int,
    month: Int,
    processedAt: Timestamp
)

case class SilverCall(
    callId: String,
    initiatorId: String,
    participantCount: Int,
    conversationId: Option[String],
    callType: String,
    status: String,
    isCompleted: Boolean,
    isMissed: Boolean,
    startTime: Option[Timestamp],
    endTime: Option[Timestamp],
    durationSeconds: Option[Long],
    durationMinutes: Option[Double],
    durationCategory: String, // short (<1min), medium (1-10min), long (>10min)
    createdAt: Timestamp,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    processedAt: Timestamp
)

case class SilverUserActivity(
    userId: String,
    activityDate: java.sql.Date,
    messagesSent: Long,
    messagesReceived: Long,
    reactionsGiven: Long,
    reactionsReceived: Long,
    callsInitiated: Long,
    callsReceived: Long,
    totalCallDurationMinutes: Double,
    conversationsActive: Long,
    newConversationsStarted: Long,
    mediaShared: Long,
    isActiveDay: Boolean,
    year: Int,
    month: Int,
    day: Int,
    dayOfWeek: Int,
    processedAt: Timestamp
)

// ============================================
// GOLD LAYER MODELS (Aggregated & Analytics)
// ============================================

case class DailyMetrics(
    metricDate: java.sql.Date,
    totalUsers: Long,
    activeUsers: Long, // DAU
    newUsers: Long,
    totalMessages: Long,
    textMessages: Long,
    mediaMessages: Long,
    totalCalls: Long,
    audioCalls: Long,
    videoCalls: Long,
    totalCallDurationMinutes: Double,
    avgCallDurationMinutes: Double,
    totalConversations: Long,
    activeConversations: Long,
    newConversations: Long,
    totalReactions: Long,
    avgMessagesPerUser: Double,
    avgSessionsPerUser: Double,
    peakHour: Int,
    year: Int,
    month: Int,
    day: Int,
    dayOfWeek: Int,
    isWeekend: Boolean,
    processedAt: Timestamp
)

case class WeeklyMetrics(
    weekStartDate: java.sql.Date,
    weekEndDate: java.sql.Date,
    weekNumber: Int,
    totalUsers: Long,
    weeklyActiveUsers: Long, // WAU
    newUsers: Long,
    returningUsers: Long,
    churnedUsers: Long,
    retentionRate: Double,
    totalMessages: Long,
    avgDailyMessages: Double,
    totalCalls: Long,
    avgDailyActiveCalls: Double,
    totalCallDurationHours: Double,
    engagementScore: Double,
    year: Int,
    processedAt: Timestamp
)

case class MonthlyMetrics(
    monthDate: java.sql.Date,
    yearMonth: String, // YYYY-MM
    totalUsers: Long,
    monthlyActiveUsers: Long, // MAU
    newUsers: Long,
    churnedUsers: Long,
    retentionRate: Double,
    dauMauRatio: Double, // Stickiness
    totalMessages: Long,
    avgDailyMessages: Double,
    totalCalls: Long,
    totalCallDurationHours: Double,
    avgMessagesPerActiveUser: Double,
    topMessageTypes: Map[String, Long],
    peakDayOfWeek: Int,
    year: Int,
    month: Int,
    processedAt: Timestamp
)

case class UserEngagementMetrics(
    userId: String,
    calculationDate: java.sql.Date,
    // Activity metrics
    totalMessagesSent: Long,
    totalMessagesReceived: Long,
    totalReactionsGiven: Long,
    totalReactionsReceived: Long,
    totalCallsInitiated: Long,
    totalCallsReceived: Long,
    totalCallDurationMinutes: Double,
    // Engagement metrics
    activeDaysLast7: Int,
    activeDaysLast30: Int,
    avgDailyMessages: Double,
    avgResponseTimeMinutes: Option[Double],
    // Network metrics
    uniqueContactsMessaged: Long,
    conversationsParticipating: Long,
    groupsParticipating: Long,
    // Derived scores
    engagementScore: Double,
    activityTrend: String, // increasing, stable, decreasing
    churnRisk: String, // low, medium, high
    userSegment: String, // power_user, regular, casual, at_risk, churned
    year: Int,
    month: Int,
    processedAt: Timestamp
)

case class ConversationAnalytics(
    conversationId: String,
    calculationDate: java.sql.Date,
    conversationType: String,
    participantCount: Int,
    totalMessages: Long,
    totalMessagesLast7Days: Long,
    totalMessagesLast30Days: Long,
    uniqueParticipantsLast7Days: Int,
    avgMessagesPerDay: Double,
    avgMessagesPerParticipant: Double,
    mostActiveParticipantId: Option[String],
    peakActivityHour: Int,
    dominantMessageType: String,
    mediaShareRate: Double,
    reactionRate: Double,
    conversationHealth: String, // thriving, active, quiet, dying, dead
    year: Int,
    month: Int,
    processedAt: Timestamp
)

// ML Feature models
case class UserFeatures(
    userId: String,
    featureDate: java.sql.Date,
    // Activity features
    msgSentLast1d: Long,
    msgSentLast7d: Long,
    msgSentLast30d: Long,
    msgRecvLast1d: Long,
    msgRecvLast7d: Long,
    msgRecvLast30d: Long,
    // Engagement features
    activeDaysLast7d: Int,
    activeDaysLast30d: Int,
    avgSessionDurationMinutes: Double,
    // Social features
    uniqueContacts: Long,
    groupCount: Long,
    avgGroupSize: Double,
    // Temporal features
    preferredHour: Int,
    weekdayActivityRatio: Double,
    // Derived features
    messageVelocity7d: Double,
    engagementTrend: Double,
    // Label (for supervised learning)
    willChurnNext7d: Option[Boolean],
    processedAt: Timestamp
)
