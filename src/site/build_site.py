"""
Build script that generates a static HTML page with all report links.
This runs during the GitHub Actions workflow after reports are created.
"""
import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv("API_BASE", "https://app.southwind.ai/api")
API_KEY = os.getenv("API_KEY", "")


def get_headers():
    """Get headers with API key for authentication."""
    headers = {}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    return headers


def wait_for_report_completion(task_id, max_wait_seconds=1800, poll_interval=30):
    """
    Poll the report status until it's completed or failed.
    
    Args:
        task_id: The report task ID
        max_wait_seconds: Maximum time to wait (default 30 minutes)
        poll_interval: Seconds between polls (default 30 seconds)
    
    Returns:
        True if completed successfully, False otherwise
    """
    print(f"Waiting for report {task_id} to complete...")
    start_time = time.time()
    attempts = 0
    
    while True:
        attempts += 1
        elapsed = time.time() - start_time
        
        if elapsed > max_wait_seconds:
            print(f"Timeout: Report did not complete within {max_wait_seconds} seconds")
            return False
        
        try:
            response = requests.get(
                f"{API_BASE}/v1/reports/{task_id}",
                headers=get_headers(),
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Error checking status: {response.status_code} - {response.text}")
                return False
            
            data = response.json()
            status = data.get("status", "unknown")
            
            print(f"Attempt {attempts}: Status = {status} (elapsed: {int(elapsed)}s)")
            
            if status == "completed":
                print(f"✓ Report {task_id} completed successfully!")
                return True
            elif status == "failed":
                print(f"✗ Report {task_id} failed")
                return False
            elif status in ["queued", "processing", "running"]:
                # Still in progress, continue polling
                time.sleep(poll_interval)
            else:
                print(f"Unknown status: {status}")
                time.sleep(poll_interval)
                
        except Exception as e:
            print(f"Error polling status: {e}")
            time.sleep(poll_interval)


def get_all_reports():
    """Fetch all reports from the API."""
    try:
        response = requests.get(
            f"{API_BASE}/v1/reports/",
            headers=get_headers(),
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Error fetching reports: {response.status_code} - {response.text}")
            return []
        
        data = response.json()
        reports = data.get("reports", [])
        print(f"Found {len(reports)} reports")
        return reports
        
    except Exception as e:
        print(f"Error fetching reports: {e}")
        return []


def get_report_embed_url(task_id):
    """Get the embed URL for a specific report."""
    try:
        response = requests.get(
            f"{API_BASE}/v1/reports/{task_id}",
            headers=get_headers(),
            params={"format": "embed"},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"Error fetching embed URL for {task_id}: {response.status_code}")
            return None
        
        data = response.json()
        return data.get("embedded_url")
        
    except Exception as e:
        print(f"Error fetching embed URL for {task_id}: {e}")
        return None


def format_italian_date(date_string):
    """Format date string in Italian."""
    try:
        from datetime import timedelta
        date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        days = ['Lunedì', 'Martedì', 'Mercoledì', 'Giovedì', 'Venerdì', 'Sabato', 'Domenica']
        months = ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno', 
                  'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']
        
        day_name = days[date_obj.weekday()]
        day = date_obj.day
        month = months[date_obj.month - 1]
        year = date_obj.year
        
        return f"{day_name} {day} {month} {year}"
    except Exception as e:
        print(f"Error formatting date {date_string}: {e}")
        return date_string


def generate_html(reports_with_urls):
    """Generate the HTML content with all report links."""
    
    # Sort reports by date (newest first)
    reports_with_urls.sort(key=lambda x: x['time'], reverse=True)
    
    # Generate report list items
    report_list_html = ""
    for report in reports_with_urls:
        if report['embed_url']:
            formatted_date = format_italian_date(report['time'])
            report_list_html += f"""            <li class="report-list__item">
              <span class="report-list__date">{formatted_date}</span>
              <a href="{report['embed_url']}" class="report-list__link" target="_blank" rel="noopener noreferrer">Leggi il report &rarr;</a>
            </li>
"""
    
    if not report_list_html:
        report_list_html = '            <li class="report-list__item"><span class="report-list__date">Nessun report disponibile</span></li>\n'
    
    # Get the latest report URL for the hero CTA
    latest_report_url = reports_with_urls[0]['embed_url'] if reports_with_urls and reports_with_urls[0]['embed_url'] else '#'
    
    # Read the template
    template_path = os.path.join(os.path.dirname(__file__), "index.template.html")
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()
    
    # Add auto-generation notice at the top
    build_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    auto_gen_comment = f"""<!--
  AUTO-GENERATED FILE - DO NOT EDIT MANUALLY
  Generated by: src/site/build_site.py
  Build time: {build_time}
  
  To modify this file, edit src/site/index.template.html and run:
  python3 src/site/build_site.py
-->
"""
    
    # Replace placeholders
    html = template.replace("<!DOCTYPE html>", f"<!DOCTYPE html>\n{auto_gen_comment}")
    html = html.replace("{{LATEST_REPORT_URL}}", latest_report_url)
    html = html.replace("{{REPORT_LIST}}", report_list_html)
    html = html.replace("{{BUILD_TIME}}", build_time)
    
    return html


def main():
    """Main build function."""
    print("=" * 60)
    print("Building static site with report links...")
    print("=" * 60)
    
    # Check if we should wait for a specific report
    new_report_id = os.getenv("NEW_REPORT_ID", "")
    if new_report_id:
        print(f"\nWaiting for new report {new_report_id} to complete...")
        if not wait_for_report_completion(new_report_id):
            print("Warning: Report did not complete successfully")
            # Continue anyway to rebuild with existing reports
    
    # Fetch all reports
    print("\nFetching all reports...")
    reports = get_all_reports()
    
    if not reports:
        print("No reports found, generating empty page")
        reports_with_urls = []
    else:
        # Fetch embed URLs for all reports
        print(f"\nFetching embed URLs for {len(reports)} reports...")
        reports_with_urls = []
        for i, report in enumerate(reports, 1):
            print(f"[{i}/{len(reports)}] Fetching embed URL for report {report['id']}...")
            embed_url = get_report_embed_url(report['id'])
            if embed_url:
                reports_with_urls.append({
                    'id': report['id'],
                    'time': report['time'],
                    'title': report.get('title', ''),
                    'embed_url': embed_url
                })
            else:
                print(f"  Warning: Could not get embed URL for report {report['id']}")
    
    # Generate HTML
    print("\nGenerating HTML...")
    html = generate_html(reports_with_urls)
    
    # Write output
    output_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✓ Generated {output_path}")
    print(f"✓ Included {len(reports_with_urls)} reports")
    print("\n" + "=" * 60)
    print("Build completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Build failed: {e}")
        sys.exit(1)

