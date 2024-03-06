import TripsPerHourPanel from "./components/TripsPerHourPanel";
import CheapestHourPanel from "./components/CheapestHourPanel";
const App = () => {
    //march and april 2020 got little data
    return (
        <div className="flex w-screen h-screen bg-slate-100 font-mono">
            <TripsPerHourPanel />
            <CheapestHourPanel />
        </div>
    );
};

export default App;
