import {FC, FormEvent, FormEventHandler} from "react";
import {AppHeaderProps} from "./AppHeader.types";
import NStreamLogo from "../../assets/nstream-logo.svg?react";

export const AppHeader: FC<AppHeaderProps> = (props) => {
    const {search, setSearch} = props;

    const handleInput: FormEventHandler<HTMLInputElement> = (e: FormEvent<HTMLInputElement>) => {
        setSearch(e.currentTarget.value.slice(0, 4).toUpperCase());
    };

    return (
        <nav className="w-full h-auto flex flex-row grow-0 shrink-0 p-4">
            <span className="text-logo dark:text-logo-dark"><NStreamLogo/></span>
            <div className="h-full flex flex-col grow shrink justify-between items-start ml-2">
                <h1 className="text-xl font-semibold">Simulated Stock Demo</h1>
                <h2 className="text-xs">v1.0.0</h2>
            </div>
            <div className="flex flex-col justify-end align-end grow-0 shrink-0 basis-auto">
                <div className="flex flex-row grow-0 shrink-0 basis-auto">
                    <label className="mr-2 text-sm" htmlFor="search">
                        Search
                    </label>
                    <input
                        className="w-[140px] border border-black/50 dark:border-white/50 rounded-sm text-sm text-primary dark:text-primary-dark font-medium bg-transparent focus-within:outline-none px-1"
                        type="text"
                        onInput={handleInput}
                        value={search}
                        name="search"
                    />
                </div>
            </div>
        </nav>
    );
};
