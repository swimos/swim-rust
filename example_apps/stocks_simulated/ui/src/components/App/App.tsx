import { FC, useState } from "react";
import { AppHeader } from "../AppHeader";
import { Table } from "../Table";

export const App: FC = () => {
  const [search, setSearch] = useState<string>("");

  return (
    <div className="flex flex-col w-[100vw] h-[100vh] max-w-[100vw] overflow-auto bg-app dark:bg-app-dark text-primary dark:text-primary-dark">
      <AppHeader search={search} setSearch={setSearch} />
      <main className="w-full grow shrink py-4">
        <Table search={search} />
      </main>
    </div>
  );
};

export default App;
