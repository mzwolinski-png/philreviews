print("Testing main function...")

try:
    import sys
    sys.argv = ['test_script', '--max-reviews', '5']  # Simulate command line args
    print(f"Simulated args: {sys.argv}")
    
    from philreviews_scraper import main
    print("About to call main()...")
    main()
    print("Main() completed!")
    
except Exception as e:
    print(f"Error in main(): {e}")
    import traceback
    traceback.print_exc()
