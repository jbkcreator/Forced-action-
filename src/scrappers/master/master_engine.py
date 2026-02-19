import asyncio
import os
import shutil
import time
from pathlib import Path
from browser_use import Agent, ChatAnthropic
from config.settings import settings

# 1. Setup the Brain (Claude)
# We use temperature=0 for scraping to ensure deterministic actions
llm = ChatAnthropic(
	model="claude-sonnet-4-5-20250929", 
	timeout=120,
    api_key=settings.anthropic_api_key.get_secret_value(),
	temperature=0
)

async def download_parcel_master():
	"""Download the PARCEL_SPREADSHEET.xls file using browser automation."""
	save_dir = os.path.abspath("data/reference/")
	os.makedirs(save_dir, exist_ok=True)

	task = (
		"Go to https://downloads.hcpafl.org/ "
		"Find and click on 'PARCEL_SPREADSHEET.xls' (536 MB file) to start the download. "
		"IMPORTANT: The click action will timeout after 15 seconds - this is NORMAL for large file downloads. "
		"After clicking, wait exactly 60 seconds for the background download to complete. "
		"Do NOT click again, do NOT try to verify in downloads tab - just wait 60 seconds after clicking and then finish."
	)

	print("[*] Launching browser agent to download PARCEL_SPREADSHEET.xls...")

	agent = Agent(
		task=task,
		llm=llm,
		browser_context_config={
			"headless": True,
			"save_downloads_path": save_dir,
		}
	)

	history = await agent.run()
	
	if not history.is_done():
		print("[!] Agent was unable to complete the task. Check logs.")
		return False
	
	print("[+] Download initiated. Checking for completed file...")
	
	# Agent already waited 60 seconds, just give a small buffer
	await asyncio.sleep(5)
	
	# Search for the downloaded file in browser-use temp directories
	downloaded_file = None
	temp_base = Path("C:/tmp")
	
	if temp_base.exists():
		for download_dir in temp_base.glob("browser-use-downloads-*"):
			# Look for completed XLS file (not .crdownload)
			for xls_file in download_dir.glob("*[Pp][Aa][Rr][Cc][Ee][Ll]*.[Xx][Ll][Ss]"):
				if not xls_file.name.endswith('.crdownload'):
					downloaded_file = xls_file
					print(f"[*] Found file: {xls_file}")
					break
			if downloaded_file:
				break
	
	if not downloaded_file or not downloaded_file.exists():
		print(f"[!] Could not locate completed download file")
		return False
	
	print(f"[*] Verifying file size...")
	
	# Quick check - file should be ~536 MB
	final_size_mb = downloaded_file.stat().st_size / (1024**2)
	
	if final_size_mb < 100:
		print(f"[!] WARNING: File size ({final_size_mb:.1f} MB) seems incomplete.")
		print(f"[*] Waiting 30 more seconds for download to finish...")
		
		# Wait and monitor for completion
		start_time = time.time()
		while time.time() - start_time < 30:
			try:
				current_size_mb = downloaded_file.stat().st_size / (1024**2)
				if current_size_mb >= 500:
					print(f"[+] Download complete: {current_size_mb:.1f} MB")
					final_size_mb = current_size_mb
					break
				print(f"[*] Progress: {current_size_mb:.1f} MB")
				time.sleep(3)
			except Exception as e:
				print(f"[!] Error: {e}")
				time.sleep(3)
		
		final_size_mb = downloaded_file.stat().st_size / (1024**2)
		if final_size_mb < 100:
			print(f"[!] ERROR: Download incomplete at {final_size_mb:.1f} MB")
			return False
	
	# Move to final destination
	dest_file = Path(save_dir) / "PARCEL_SPREADSHEET.xls"
	print(f"[*] Moving file to {dest_file}")
	shutil.move(str(downloaded_file), str(dest_file))
	
	# Clean up temp directory
	try:
		temp_dir = downloaded_file.parent
		if temp_dir.exists() and temp_dir.name.startswith("browser-use-downloads-"):
			shutil.rmtree(str(temp_dir))
			print(f"[*] Cleaned up temp directory: {temp_dir}")
	except Exception as e:
		print(f"[!] Warning: Could not clean up temp dir: {e}")
	
	print(f"[+] Success! File saved to {dest_file} ({final_size_mb:.1f} MB)")
	return True


if __name__ == "__main__":
	asyncio.run(download_parcel_master())

