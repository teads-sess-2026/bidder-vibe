import { Routes } from '@angular/router';
import {NoteView} from "./views/note/note.view";
import {AboutView} from "./views/about/about.view";
import {HomeView} from "./views/home/home.view";

export const routes: Routes = [
    { path: 'note/:id', component: NoteView },
    { path: 'about', component: AboutView },
    { path: '', component: HomeView }
];
