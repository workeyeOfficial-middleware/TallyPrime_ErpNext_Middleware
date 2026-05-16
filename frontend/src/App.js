import { useEffect, useState, useRef } from "react";
import DashboardWrapper from "./DashboardWrapper";
import { tallyAPI } from "./api/tallyAPI";

function App() {
  const [companies, setCompanies] = useState([]);
  const retryRef = useRef(null); // holds the interval so we can clear it

  useEffect(() => {
    async function loadCompanies() {
      try {
        const res = await tallyAPI.companies();
        console.log("Companies API:", res);

        const list = res?.data || res || [];

        // Got a valid response with companies — stop retrying
        if (list.length > 0) {
          clearInterval(retryRef.current);
          retryRef.current = null;
          setCompanies(list);
        }
        // If list is empty (Tally open but no company loaded), keep retrying
      } catch (err) {
        // Tally not reachable yet — retry will handle it, no need to log every attempt
        console.log("Tally not reachable, will retry...");
      }
    }

    // Run immediately on mount
    loadCompanies();

    // Then retry every 5 seconds until companies are found
    retryRef.current = setInterval(loadCompanies, 5000);

    // Cleanup on unmount
    return () => clearInterval(retryRef.current);
  }, []);

  return <DashboardWrapper companies={companies} />;
}

export default App;