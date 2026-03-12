# NEWWORLD Master Project File (Updated)

## System Blueprint

![System Blueprint](/mnt/data/NEWWORLD_SYSTEM_BLUEPRINT.png)

The diagram above illustrates the full architecture of the NEWWORLD
platform.

### System Flow

Users interact with the frontend dashboard, which communicates with a
FastAPI backend.\
The backend processes deal intelligence, historical price data, and
analytics.

Core layers:

Users → Frontend → FastAPI → Redis Cache → PostgreSQL

Supporting systems include:

• Steam scrapers that collect price and metadata\
• Worker queues that update pricing and analytics\
• AI systems that generate sale predictions\
• Analytics warehouse for large-scale insights

------------------------------------------------------------------------

## Platform Purpose

NEWWORLD aims to become:

"The ultimate master hub for gamers combining deal tracking, game
analytics, guides, calculators, LFG systems, community features, and
AI-driven gaming intelligence."

------------------------------------------------------------------------

## Core Technology Stack

Backend: - Python - FastAPI - SQLAlchemy - PostgreSQL

Frontend: - HTML - CSS - JavaScript

Infrastructure: - Uvicorn - Redis (planned) - Worker queues - Scraper
systems

------------------------------------------------------------------------

## Core Data Tables

games\
game_prices\
wishlist_items\
watchlist_items\
price_alerts

These tables power:

• historical price tracking\
• deal ranking\
• player analytics\
• wishlist notifications

------------------------------------------------------------------------

## Key Platform Features

Steam deal tracking\
price history tracking\
historical low detection\
deal ranking algorithm\
seasonal sale radar\
wishlist / watchlist\
trending player analytics\
Steam banner integration\
infinite scroll UI\
advanced filtering

------------------------------------------------------------------------

## Immediate Development Priorities

1.  Price history graphs
2.  Game detail pages
3.  Deal timeline visualization
4.  Improved trending algorithm
5.  Smart wishlist insights
6.  Tag explorer pages
7.  Market insights dashboard

------------------------------------------------------------------------

## Future Expansion

AI deal prediction\
cross-store pricing\
gaming calculators\
strategy guides\
community systems\
analytics dashboards\
recommendation engine

------------------------------------------------------------------------

## Long-Term Vision

NEWWORLD becomes a unified gaming platform combining:

• SteamDB-style analytics\
• IsThereAnyDeal-style price tracking\
• Maxroll-style guides\
• LFG community tools\
• advanced gaming calculators
