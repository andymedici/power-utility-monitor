# migrate_database.py - Add hunter_score fields to existing database
"""
Run this script to add hunter_score and hunter_notes columns to your database.
Usage: python migrate_database.py
"""

import os
import sys
from datetime import datetime

# Fix Railway PostgreSQL URL
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# Create minimal Flask app for database access
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


def run_migration():
    """Add hunter_score and hunter_notes columns"""
    
    with app.app_context():
        print("🔄 Starting database migration...")
        
        try:
            # Check if columns already exist
            from sqlalchemy import inspect
            inspector = inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('power_projects')]
            
            needs_hunter_score = 'hunter_score' not in columns
            needs_hunter_notes = 'hunter_notes' not in columns
            
            if not needs_hunter_score and not needs_hunter_notes:
                print("✅ Database already has hunter_score fields. No migration needed.")
                return True
            
            # Add columns
            if needs_hunter_score:
                print("Adding hunter_score column...")
                db.session.execute('ALTER TABLE power_projects ADD COLUMN hunter_score INTEGER DEFAULT 0')
                print("✓ Added hunter_score")
            
            if needs_hunter_notes:
                print("Adding hunter_notes column...")
                db.session.execute('ALTER TABLE power_projects ADD COLUMN hunter_notes TEXT')
                print("✓ Added hunter_notes")
            
            db.session.commit()
            
            print("\n✅ Migration completed successfully!")
            print("\n📊 Next steps:")
            print("1. Run a full scan to calculate hunter scores for existing projects")
            print("2. Visit /projects?filter=hunter to see high-confidence data center projects")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Migration failed: {e}")
            print("\nTroubleshooting:")
            print("- Make sure DATABASE_URL is set correctly")
            print("- Check that you have database write permissions")
            print("- For PostgreSQL, you may need to run as database owner")
            db.session.rollback()
            return False


def backfill_scores():
    """
    Optional: Recalculate hunter scores for all existing projects
    Run this after the migration to score your existing data
    """
    from enhanced_monitor import EnhancedPowerMonitor
    from app import PowerProject
    
    with app.app_context():
        print("\n🎯 Backfilling hunter scores for existing projects...")
        
        monitor = EnhancedPowerMonitor()
        projects = PowerProject.query.all()
        
        print(f"Found {len(projects)} projects to score...")
        
        updated = 0
        for project in projects:
            # Build project data dict
            project_data = {
                'project_name': project.project_name or '',
                'customer': project.customer or '',
                'fuel_type': project.fuel_type or '',
                'capacity_mw': project.capacity_mw or 0,
                'county': project.county or '',
                'state': project.state or ''
            }
            
            # Calculate score
            score_result = monitor.calculate_hunter_score(project_data)
            
            # Update project
            project.hunter_score = score_result['hunter_score']
            project.hunter_notes = score_result['hunter_notes']
            
            # Update type if high confidence data center
            if score_result['hunter_score'] >= 60:
                project.project_type = 'datacenter'
            
            updated += 1
            
            if updated % 100 == 0:
                print(f"  Processed {updated} projects...")
                db.session.commit()
        
        db.session.commit()
        
        print(f"\n✅ Updated {updated} projects with hunter scores")
        
        # Show statistics
        high_conf = PowerProject.query.filter(PowerProject.hunter_score >= 70).count()
        medium_conf = PowerProject.query.filter(PowerProject.hunter_score >= 40, PowerProject.hunter_score < 70).count()
        
        print(f"\n📊 Hunter Score Distribution:")
        print(f"  High confidence (70+): {high_conf} projects")
        print(f"  Medium confidence (40-69): {medium_conf} projects")
        print(f"  Low confidence (<40): {updated - high_conf - medium_conf} projects")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate database to add hunter score fields')
    parser.add_argument('--backfill', action='store_true', help='Also recalculate scores for existing projects')
    args = parser.parse_args()
    
    print("="*60)
    print("DATABASE MIGRATION - Add Hunter Score Fields")
    print("="*60)
    
    # Run migration
    success = run_migration()
    
    # Optionally backfill scores
    if success and args.backfill:
        print("\n" + "="*60)
        backfill_scores()
        print("="*60)
    elif success:
        print("\nℹ️  To calculate scores for existing projects, run:")
        print("  python migrate_database.py --backfill")
    
    sys.exit(0 if success else 1)
