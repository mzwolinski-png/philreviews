print("Script started!")

try:
    from philreviews_scraper import PhilReviewsScraper
    print("Import successful!")

    scraper = PhilReviewsScraper()
    print("Scraper created!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("Script finished!")
