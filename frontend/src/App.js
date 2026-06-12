import { useEffect, useState, useRef } from "react";
import DashboardWrapper from "./DashboardWrapper";
import { tallyAPI } from "./api/tallyAPI";

// REPLACE WITH THIS:
function App() {
  const [companies, setCompanies] = useState([]);
  const [currentCompany, setCurrentCompany] = useState(
    () => localStorage.getItem("last_tally_company") || ""
  );
  const retryRef = useRef(null);

  async function loadCompanies() {
    try {
      const res = await tallyAPI.companies();
      const list = res?.data || res || [];
      if (list.length > 0) {
        clearInterval(retryRef.current);
        retryRef.current = null;
        setCompanies(list);
      }
    } catch (err) {
      console.log("Tally not reachable, will retry...");
    }
  }

  useEffect(() => {
    loadCompanies();
    retryRef.current = setInterval(loadCompanies, 5000);
    return () => clearInterval(retryRef.current);
  }, []);

  return (
    <DashboardWrapper
      companies={companies}
      onRefresh={loadCompanies}
      selectedCompany={currentCompany}
      onCompanyChange={(co) => setCurrentCompany(co)}
    />
  );
}


export default App;